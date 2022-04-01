/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.time.Instant;
import java.util.Set;

class VolumeDetailsMessage {
    private final Instant snapshotAt;
    private final Set<VolumeDetails> volumeDetails;

    public VolumeDetailsMessage(Instant snapshotAt, Set<VolumeDetails> volumeDetails) {
        this.snapshotAt = snapshotAt;
        this.volumeDetails = volumeDetails;
    }

    public Instant getSnapshotAt() {
        return snapshotAt;
    }

    public Set<VolumeDetails> getVolumeDetails() {
        return volumeDetails;
    }
}
