/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaDataSourceTask implements DataSourceTask {

    private final Collection<Path> logDirs;
    private final long period;
    private final TimeUnit timeUnit;
    private final VolumeMetricsPublisher volumeMetricsPublisher;

    public KafkaDataSourceTask(Collection<Path> logDirs, long period, TimeUnit timeUnit, VolumeMetricsPublisher volumeMetricsPublisher) {
        this.logDirs = logDirs;
        this.period = period;
        this.timeUnit = timeUnit;
        this.volumeMetricsPublisher = volumeMetricsPublisher;
    }

    @Override
    public void run() {
        final Set<VolumeDetails> volumeDetails = logDirs.stream()
                .filter(Files::exists) // WHat if it doesn't?
                .map(path -> apply(() -> Files.getFileStore(path)))
                .distinct() // In a strimzi deployment this should have no effect as each logdir should be a separate claim
                .map(this::convertToVolumeDetails)
                .collect(Collectors.toSet());

        volumeMetricsPublisher.send(volumeDetails);
    }

    @Override
    public long getPeriod() {
        return period;
    }

    @Override
    public TimeUnit getPeriodUnit() {
        return timeUnit;
    }

    private VolumeDetails convertToVolumeDetails(FileStore fs) {
        try {
            return new VolumeDetails(fs.name(), fs.getTotalSpace(), fs.getTotalSpace() - fs.getUsableSpace());
        } catch (IOException e) {
            return VolumeDetails.unknownVolumeDetails(fs.name());
        }
    }

    static <T> T apply(IOSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @FunctionalInterface
    interface IOSupplier<T> {
        T get() throws IOException;
    }
}
