/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
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
    private final Set<ClientQuotaType> resetQuota = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final StorageChecker storageChecker;
    private final static long LOGGING_DELAY_MS = 1000;
    private AtomicLong lastLoggedMessageSoftTimeMs = new AtomicLong(0);
    private AtomicLong lastLoggedMessageHardTimeMs = new AtomicLong(0);
    private static final String METRICS_SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";

    private final ScheduledExecutorService backgroundScheduler;

    //Default to no restrictions until things have been configured.
    private QuotaSupplier quotaSupplier = UnlimitedThrottleSupplier.UNLIMITED_QUOTA_SUPPLIER;
    private ThrottleFactorSupplier throttleFactorSupplier = UnlimitedThrottleSupplier.UNLIMITED_QUOTA_SUPPLIER;


    public StaticQuotaCallback() {
        this(new StorageChecker(), Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, StaticQuotaCallback.class.getSimpleName() + "-taskExecutor");
            thread.setDaemon(true);
            return thread;
        }));
    }

    /*test*/ StaticQuotaCallback(StorageChecker storageChecker, ScheduledExecutorService backgroundScheduler) {
        this.storageChecker = storageChecker;
        this.backgroundScheduler = backgroundScheduler;
        Collections.addAll(resetQuota, ClientQuotaType.values());
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
        final double limit;
        if (Boolean.TRUE.toString().equals(metricTags.get(EXCLUDED_PRINCIPAL_QUOTA_KEY))) {
            limit = QuotaSupplier.UNLIMITED;
        } else {
            final double requestQuota = quotaSupplier.quotaFor(quotaType, metricTags);
            if (ClientQuotaType.PRODUCE.equals(quotaType)) {
                //Kafka will suffer an A divide by zero if returned 0.0 from `quotaLimit` so ensure that we don't even if we have zero quota available
                limit = Math.max(requestQuota * throttleFactorSupplier.get(), QuotaSupplier.PAUSED);
            } else {
                limit = requestQuota;
            }
        }

        log.trace("Quota limit for metric tags {} : {}", metricTags, limit);
        return limit;
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
        return resetQuota.remove(quotaType);
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {
        try {
            closeExecutorService();
        } finally {
            Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> METRICS_SCOPE.equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        storageQuotaSoft = config.getSoftStorageQuota();
        storageQuotaHard = config.getHardStorageQuota();
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();

        quotaSupplier = config.quotaSupplier();
        throttleFactorSupplier = config.throttleFactorSupplier();
        throttleFactorSupplier.addUpdateListener(() -> resetQuota.add(ClientQuotaType.PRODUCE));

        long storageCheckIntervalMillis = TimeUnit.SECONDS.toMillis(config.getStorageCheckInterval());

        if (storageCheckIntervalMillis > 0L) {
            List<Path> logDirs = config.getLogDirs().stream().map(Paths::get).collect(Collectors.toList());
            storageChecker.configure(
                    logDirs,
                    config.throttleFactorCalculator()
            );
            backgroundScheduler.scheduleWithFixedDelay(storageChecker, storageCheckIntervalMillis, storageCheckIntervalMillis, TimeUnit.MILLISECONDS);
            log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}ms", quotaMap, storageQuotaSoft, storageQuotaHard, storageCheckIntervalMillis);
        }
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

        quotaMap.forEach((clientQuotaType, quota) -> {
            String name = clientQuotaType.name().toUpperCase(ENGLISH).charAt(0) + clientQuotaType.name().toLowerCase(ENGLISH).substring(1);
            Metrics.newGauge(metricName(StaticQuotaCallback.class, name), new ClientQuotaGauge(quota));
        });
    }

    private void closeExecutorService() {
        try {
            backgroundScheduler.shutdownNow();
        } catch (Exception e) {
            log.warn("Encountered problem shutting down background executor: {}", e.getMessage(), e);
        }
    }

    private void updateUsedStorage(Long newValue) {
        var oldValue = storageUsed.getAndSet(newValue);
        if (oldValue != newValue) {
            resetQuota.add(ClientQuotaType.PRODUCE);
        }
    }

    public static MetricName metricName(Class<?> clazz, String name) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        String mBeanName = String.format("%s:type=%s,name=%s", group, type, name);
        return new MetricName(group, type, name, METRICS_SCOPE, mBeanName);
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
