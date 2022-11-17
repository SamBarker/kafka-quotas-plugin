/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class LocalQuotaFactorSupplier implements QuotaFactorSupplier, Consumer<Double> {
    private final AtomicLong currentFactor = new AtomicLong();
    private final Set<Runnable> listeners = new CopyOnWriteArraySet<>();
    private static final double EPSILON = 0.00001;

    @Override
    public Double get() {
        return Double.longBitsToDouble(currentFactor.get());
    }

    @Override
    public void accept(Double updatedQuotaFactor) {
        final double originalFactor = get();
        if (hasChanged(updatedQuotaFactor, originalFactor)) {
            this.currentFactor.set(Double.doubleToLongBits(updatedQuotaFactor));
            listeners.forEach(Runnable::run);
        }
    }

    @Override
    public void addUpdateListener(Runnable listener) {
        listeners.add(listener);
        listener.run();
    }

    private boolean hasChanged(double newFactor, double oldFactor) {
        return Math.abs(newFactor - oldFactor) > EPSILON;
    }
}
