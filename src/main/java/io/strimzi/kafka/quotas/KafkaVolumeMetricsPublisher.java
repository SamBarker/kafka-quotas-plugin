/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.time.Instant;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaVolumeMetricsPublisher implements VolumeMetricsPublisher {

    private final KafkaProducer<String, VolumeDetailsMessage> kafkaProducer;
    private final String topic;
    private final String key;

    public KafkaVolumeMetricsPublisher(KafkaProducer<String, VolumeDetailsMessage> kafkaProducer, String topic, String key) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        this.key = key;
    }

    @Override
    public void send(Instant snapshotAt, Set<VolumeDetails> volumeDetails) {
        kafkaProducer.send(new ProducerRecord<>(topic, key, new VolumeDetailsMessage(snapshotAt, volumeDetails)));
    }
}
