/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TotalStorageThrottleFactorCalculatorTest {

    private TotalStorageThrottleFactorCalculator totalStorageThrottleFactorCalculator;

    @BeforeEach
    void setUp() {
        totalStorageThrottleFactorCalculator = new TotalStorageThrottleFactorCalculator(10L, 15L);
    }

    @Test
    void shouldCalculate100PercentWhenUnderLimit() {
        //Given
        AtomicReference<Double> actualFactor = new AtomicReference<>();
        totalStorageThrottleFactorCalculator.addListener(actualFactor::set);

        //When
        totalStorageThrottleFactorCalculator.accept(List.of(new Volume("/data", 50L, 5L)));

        //Then
        assertThat(actualFactor).hasValue(1.0);
    }

    @Test
    void shouldCalculate0PercentWhenOverHardLimit() {
        //Given
        AtomicReference<Double> actualFactor = new AtomicReference<>();
        totalStorageThrottleFactorCalculator.addListener(actualFactor::set);

        //When
        totalStorageThrottleFactorCalculator.accept(List.of(new Volume("/data", 50L, 16L)));

        //Then
        assertThat(actualFactor).hasValue(0.0);
    }


    @Test
    void shouldThrottleWhenSoftLimitExceeded() {
        //Given
        AtomicReference<Double> actualFactor = new AtomicReference<>();
        totalStorageThrottleFactorCalculator.addListener(actualFactor::set);

        //When
        totalStorageThrottleFactorCalculator.accept(List.of(new Volume("/data", 50L, 12L)));

        //Then
        assertThat(actualFactor).hasValue(0.6);
    }
}
