/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StubVolumeMetricsPublisher implements VolumeMetricsPublisher {
    private final List<Set<VolumeDetails>> updates = new ArrayList<>();

    @Override
    public void send(Instant snapshotAt, Set<VolumeDetails> volumeDetails) {
        updates.add(volumeDetails);
    }

    public List<Set<VolumeDetails>> getUpdates() {
        return updates;
    }
}
