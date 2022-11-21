/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.function.Consumer;

public interface ThrottleFactorCalculator extends Consumer<Collection<Volume>> {

    void addListener(Consumer<Double> listener);
}
