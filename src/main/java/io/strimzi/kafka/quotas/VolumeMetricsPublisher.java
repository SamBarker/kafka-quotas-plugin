/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.time.Instant;
import java.util.Set;

public interface VolumeMetricsPublisher {
    void send(Instant snapshotAt, Set<VolumeDetails> volumeDetails);
}
