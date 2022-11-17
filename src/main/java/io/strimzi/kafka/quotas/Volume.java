/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

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
}
