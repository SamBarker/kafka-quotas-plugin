/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.iterable;

class PublishingDataSourceTaskTest {


    private static final long TEN_GB = 10737418240L;
    private StubVolumeMetricsPublisher kafkaPublisher;
    private Path logDir;
    private static FileSystem fileSystem;

    @BeforeAll
    static void configureFileSystem() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix().toBuilder().setMaxSize(TEN_GB).build());
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        kafkaPublisher = new StubVolumeMetricsPublisher();
        logDir = fileSystem.getPath("logdir");
        if (!Files.exists(logDir)) {
            Files.createDirectory(logDir);
        }
    }

    @Test
    void shouldPublishVolumeUsageMetrics() throws IOException {
        //Given
        final PublishingDataSourceTask sourceTask = new PublishingDataSourceTask(Set.of(logDir), 30, TimeUnit.SECONDS, kafkaPublisher);
        final FileStore fileStore = Files.getFileStore(logDir);
        final VolumeDetails expected = new VolumeDetails(fileStore.name(), TEN_GB, 0L);

        //When
        sourceTask.run();

        //Then
        assertThat(kafkaPublisher.getUpdates())
                .singleElement(iterable(VolumeDetails.class))
                .singleElement()
                .isEqualTo(expected);
    }

    @Test
    void shouldPublishVolumeUsageMetricsForMultipleLogDirs(@TempDir Path additionalLogDir) {
        //Given
        final PublishingDataSourceTask sourceTask = new PublishingDataSourceTask(Set.of(logDir, additionalLogDir), 30, TimeUnit.SECONDS, kafkaPublisher);

        //When
        sourceTask.run();

        //Then
        assertThat(kafkaPublisher.getUpdates())
                .singleElement(iterable(VolumeDetails.class))
                .hasSize(2);
        //This assertion is a bit rubbish, but jimfs doesn't allow us to mock multiple disks as the filesystem name is hardcoded to jimfs
        //So we can assert the number of elements or reproduce the prod code with a bonus race condition.
    }

    @Test
    void shouldNotPublishIfLogDirDoesntExist() {
        //Given
        final PublishingDataSourceTask sourceTask = new PublishingDataSourceTask(Set.of(fileSystem.getPath("logdir2")), 30, TimeUnit.SECONDS, kafkaPublisher);

        //When
        sourceTask.run();

        //Then
        assertThat(kafkaPublisher.getUpdates())
                .singleElement(iterable(VolumeDetails.class))
                .isEmpty();
    }
}
