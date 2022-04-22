/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import io.strimzi.kafka.quotas.QuotaFactorSupplier;
import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class KafkaQuotaFactorSupplier implements QuotaFactorSupplier, AutoCloseable, Runnable {
    private final Pattern subscriptionPattern;

    private final Consumer<String, UpdateQuotaFactor> kafkaConsumer;

    private final AtomicLong currentFactor;

    private final List<Runnable> listeners = new ArrayList<>();

    private static final double EPSILON = 0.00001;

    public KafkaQuotaFactorSupplier(String subscriptionPattern, Consumer<String, UpdateQuotaFactor> kafkaConsumer) {
        this.subscriptionPattern = Pattern.compile(subscriptionPattern);
        this.kafkaConsumer = kafkaConsumer;
        currentFactor = new AtomicLong(Double.doubleToLongBits(0.0));
    }

    //TODO should this be on the Interface? Will all implementations use polling? No. So how do we start it?
    public void start() {
        kafkaConsumer.subscribe(subscriptionPattern);
    }

    @Override
    public Double get() {
        return Double.longBitsToDouble(currentFactor.get());
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

    @Override
    public void run() {
        //TODO inject duration
        final ConsumerRecords<String, UpdateQuotaFactor> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10));
        final double originalFactor = get();
        consumerRecords.forEach(cr -> {
            final UpdateQuotaFactor updateMessage = cr.value();
            currentFactor.getAndSet(Double.doubleToLongBits(updateMessage.getFactor()));
        });
        if (hasChanged(currentFactor.get(), originalFactor)) {
            listeners.forEach(Runnable::run);
        }

    }

    @Override
    public void addUpdateListener(Runnable listener) {
        listeners.add(listener);
    }

    private boolean hasChanged(double newFactor, double oldFactor) {
        return Math.abs(newFactor - oldFactor) > EPSILON;
    }
}
