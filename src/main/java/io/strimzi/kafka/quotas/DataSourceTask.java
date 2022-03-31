/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.concurrent.TimeUnit;

/**
 * A task to be executed at a fixed interval to report the current volume usage statistics
 */
public interface DataSourceTask extends Runnable {

    @Override
    void run();

    long getDelay();

    TimeUnit getPeriodUnit();
}
