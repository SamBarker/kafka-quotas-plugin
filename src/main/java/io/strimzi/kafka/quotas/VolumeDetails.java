/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Objects;

public class VolumeDetails {

    private final String volumeName;
    private final long capacity;
    private final long consumed;

    public VolumeDetails(String volumeName, long capacity, long consumed) {
        this.volumeName = volumeName;
        this.capacity = capacity;
        this.consumed = consumed;
    }

    public static VolumeDetails unknownVolumeDetails(String volumeName) {
        return new VolumeDetails(volumeName, -1, -1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VolumeDetails that = (VolumeDetails) o;
        return capacity == that.capacity && consumed == that.consumed && Objects.equals(volumeName, that.volumeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(volumeName, capacity, consumed);
    }

    @Override
    public String toString() {
        return "VolumeDetails{" +
                "volumeName='" + volumeName + '\'' +
                ", capacity=" + capacity +
                ", consumed=" + consumed +
                '}';
    }
}
