/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import io.strimzi.kafka.quotas.policy.CombinedQuotaFactorPolicy;
import io.strimzi.kafka.quotas.policy.ConsumedBytesLimitPolicy;

import static io.strimzi.kafka.quotas.StaticQuotaCallback.metricName;

public class TotalStorageThrottleFactorCalculator implements ThrottleFactorCalculator {

    private final CombinedQuotaFactorPolicy combinedQuotaFactorPolicy;

    private final Set<Consumer<Double>> listeners = new CopyOnWriteArraySet<>();

    private final AtomicLong storageUsed;

    public TotalStorageThrottleFactorCalculator(long softLimit, long hardLimit) {
        final ConsumedBytesLimitPolicy softLimitPolicy = new ConsumedBytesLimitPolicy(softLimit);
        final ConsumedBytesLimitPolicy hardLimitPolicy = new ConsumedBytesLimitPolicy(hardLimit);
        combinedQuotaFactorPolicy = new CombinedQuotaFactorPolicy(softLimitPolicy, hardLimitPolicy);

        storageUsed = new AtomicLong();
        Metrics.newGauge(metricName(StorageChecker.class, "TotalStorageUsedBytes"), new Gauge<Long>() {
            public Long value() {
                return storageUsed.get();
            }
        });
        Metrics.newGauge(metricName(StorageChecker.class, "SoftLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return softLimit;
            }
        });
        Metrics.newGauge(metricName(StorageChecker.class, "HardLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return hardLimit;
            }
        });
    }

    @Override
    public void accept(Collection<Volume> volumes) {
        final long totalConsumedSpace = volumes.stream().mapToLong(Volume::getConsumed).sum();
        final long totalCapacity = volumes.stream().mapToLong(Volume::getCapacity).sum();
        final Volume synthetic = new Volume("synthetic", totalCapacity, totalConsumedSpace);
        storageUsed.set(totalConsumedSpace);
        final double newFactor = combinedQuotaFactorPolicy.quotaFactor(synthetic);
        for (Consumer<Double> listener : listeners) {
            listener.accept(newFactor);
        }
    }

    @Override
    public void addListener(Consumer<Double> listener) {
        listeners.add(listener);
    }
}
