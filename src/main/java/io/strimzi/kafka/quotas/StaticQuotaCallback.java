/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Locale.ENGLISH;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 */
public class StaticQuotaCallback implements ClientQuotaCallback {
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private static final String EXCLUDED_PRINCIPAL_QUOTA_KEY = "excluded-principal-quota-key";

    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private final AtomicLong storageUsed = new AtomicLong(0);
    private volatile long storageQuotaSoft = Long.MAX_VALUE;
    private volatile long storageQuotaHard = Long.MAX_VALUE;
    private volatile List<String> excludedPrincipalNameList = List.of();
    private final AtomicBoolean resetQuota = new AtomicBoolean(true);
    private final StorageChecker storageChecker;
    private final ScheduledExecutorService executorService;
    private final Supplier<KafkaProducer<String, VolumeDetailsMessage>> kafkaPublisherFactory;
    private final static long LOGGING_DELAY_MS = 1000;
    private AtomicLong lastLoggedMessageSoftTimeMs = new AtomicLong(0);
    private AtomicLong lastLoggedMessageHardTimeMs = new AtomicLong(0);
    private final String scope = "io.strimzi.kafka.quotas.StaticQuotaCallback";
    private volatile ScheduledFuture<?> scheduledDataSourceTask;
    private final ObjectMapper mapper;

    public StaticQuotaCallback() {
        this(new StorageChecker(), createDefaultExecutorService(), () -> new KafkaProducer<>(Map.of(), new StringSerializer(), (topic, data) -> {
            try {
                //TODO wire in shared mapper
                return new ObjectMapper().writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                log.warn("mapper screwed up", e);
                throw new SerializationException("Error when serializing volume details message", e);
            }
        }));
    }

    StaticQuotaCallback(StorageChecker storageChecker, ScheduledExecutorService executorService, Supplier<KafkaProducer<String, VolumeDetailsMessage>> kafkaPublisherFactory) {
        this.storageChecker = storageChecker;
        this.executorService = executorService;
        this.kafkaPublisherFactory = kafkaPublisherFactory;
        mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); //Ensures we use ISO-8601
    }

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        if (!excludedPrincipalNameList.isEmpty() && principal != null && excludedPrincipalNameList.contains(principal.getName())) {
            m.put(EXCLUDED_PRINCIPAL_QUOTA_KEY, Boolean.TRUE.toString());
        }
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        if (Boolean.TRUE.toString().equals(metricTags.get(EXCLUDED_PRINCIPAL_QUOTA_KEY))) {
            return Quota.upperBound(Double.MAX_VALUE).bound();
        }

        // Don't allow producing messages if we're beyond the storage limit.
        long currentStorageUsage = storageUsed.get();
        if (ClientQuotaType.PRODUCE.equals(quotaType) && currentStorageUsage > storageQuotaSoft && currentStorageUsage < storageQuotaHard) {
            double minThrottle = quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
            double limit = minThrottle * (1.0 - (1.0 * (currentStorageUsage - storageQuotaSoft) / (storageQuotaHard - storageQuotaSoft)));
            maybeLog(lastLoggedMessageSoftTimeMs, "Throttling producer rate because disk is beyond soft limit. Used: {}. Quota: {}", storageUsed, limit);
            return limit;
        } else if (ClientQuotaType.PRODUCE.equals(quotaType) && currentStorageUsage >= storageQuotaHard) {
            maybeLog(lastLoggedMessageHardTimeMs, "Limiting producer rate because disk is full. Used: {}. Limit: {}", storageUsed, storageQuotaHard);
            return 1.0;
        }
        return quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
    }

    /**
     * Put a small delay between logging
     */
    private void maybeLog(AtomicLong lastLoggedMessageTimeMs, String format, Object... args) {
        if (log.isDebugEnabled()) {
            long now = System.currentTimeMillis();
            final boolean[] shouldLog = {true};
            lastLoggedMessageTimeMs.getAndUpdate(current -> {
                if (now - current >= LOGGING_DELAY_MS) {
                    shouldLog[0] = true;
                    return now;
                }
                shouldLog[0] = false;
                return current;
            });
            if (shouldLog[0]) {
                log.debug(format, args);
            }
        }
    }

    @Override
    public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public boolean quotaResetRequired(ClientQuotaType quotaType) {
        return resetQuota.getAndSet(false);
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        storageChecker.startIfNecessary();
        return false;
    }

    @Override
    public void close() {
        try {
            storageChecker.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> scope.equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        storageQuotaSoft = config.getSoftStorageQuota();
        storageQuotaHard = config.getHardStorageQuota();
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();

        long storageCheckIntervalMillis = TimeUnit.SECONDS.toMillis(config.getStorageCheckInterval());
        List<Path> logDirs = config.getLogDirs().stream().map(Paths::get).collect(Collectors.toList());
        storageChecker.configure(storageCheckIntervalMillis,
                logDirs,
                this::updateUsedStorage);
        if (scheduledDataSourceTask != null) {
            scheduledDataSourceTask.cancel(false);
            //We don't care what the return value is we just want to ensure it will not re-trigger
        }

        final KafkaProducer<String, VolumeDetailsMessage> kafkaProducer = kafkaPublisherFactory.get();
        final PublishingDataSourceTask publishingDataSourceTask = new PublishingDataSourceTask(logDirs, config.getStorageCheckInterval(), TimeUnit.SECONDS, new KafkaVolumeMetricsPublisher(kafkaProducer, config.getVolumeMetricsTopic(), config.getBrokerId()));
        scheduledDataSourceTask = executorService.scheduleWithFixedDelay(publishingDataSourceTask, 0L, publishingDataSourceTask.getDelay(), publishingDataSourceTask.getPeriodUnit());
        log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}ms", quotaMap, storageQuotaSoft, storageQuotaHard, storageCheckIntervalMillis);
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

        Metrics.newGauge(metricName(StorageChecker.class, "TotalStorageUsedBytes"), new Gauge<Long>() {
            public Long value() {
                return storageUsed.get();
            }
        });
        Metrics.newGauge(metricName(StorageChecker.class, "SoftLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return storageQuotaSoft;
            }
        });
        Metrics.newGauge(metricName(StorageChecker.class, "HardLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return storageQuotaHard;
            }
        });

        quotaMap.forEach((clientQuotaType, quota) -> {
            String name = clientQuotaType.name().toUpperCase(ENGLISH).charAt(0) + clientQuotaType.name().toLowerCase(ENGLISH).substring(1);
            Metrics.newGauge(metricName(StaticQuotaCallback.class, name), new ClientQuotaGauge(quota));
        });
    }

    private void updateUsedStorage(Long newValue) {
        var oldValue = storageUsed.getAndSet(newValue);
        if (oldValue != newValue) {
            resetQuota.set(true);
        }
    }

    private MetricName metricName(Class<?> clazz, String name) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        String mBeanName = String.format("%s:type=%s,name=%s", group, type, name);
        return new MetricName(group, type, name, this.scope, mBeanName);
    }

    private static ScheduledExecutorService createDefaultExecutorService() {

        return Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread backgroundExecutor = new Thread(r, "StaticQuotaCallbackBackgroundExecutor");
            backgroundExecutor.setDaemon(true);
            return backgroundExecutor;
        });
    }

    private static class ClientQuotaGauge extends Gauge<Double> {
        private final Quota quota;

        public ClientQuotaGauge(Quota quota) {
            this.quota = quota;
        }

        public Double value() {
            return quota.bound();
        }
    }
}
