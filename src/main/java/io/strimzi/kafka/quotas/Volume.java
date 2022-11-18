/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Objects;
import java.util.OptionalLong;

public class Volume {

    private final String path;
    private final long capacity;
    private final long consumedSpace;

    public Volume(String path, OptionalLong capacity, OptionalLong consumedSpace) {
        this(path, capacity.orElse(0), consumedSpace.orElse(0));
    }

    public Volume(String path, long capacity, long consumedSpace) {
        this.path = path;
        this.capacity = capacity;
        this.consumedSpace = consumedSpace;
    }

    public String getPath() {
        return path;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getConsumed() {
        return consumedSpace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Volume volume = (Volume) o;
        return capacity == volume.capacity && consumedSpace == volume.consumedSpace && Objects.equals(path, volume.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, capacity, consumedSpace);
    }

    @Override
    public String toString() {
        return "Volume{" +
                "path='" + path + '\'' +
                ", capacity=" + capacity +
                ", consumedSpace=" + consumedSpace +
                '}';
    }
}
