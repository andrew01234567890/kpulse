package dev.kpulse.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.kpulse.format.KafkaEntryFormatter;
import dev.kpulse.storage.KafkaTopicManager;
import dev.kpulse.storage.PartitionLog;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives the kpulse handler on an in-process broker with a real Kafka producer and consumer: produce
 * N records, read them back with {@code assign()} (no group coordinator in M1), and assert values,
 * offsets, and that the bytes actually persisted through Pulsar's managed ledger.
 */
class KafkaVerticalSliceIntegrationTest {

    private static final String TOPIC = "vertical-slice";
    private static final int RECORD_COUNT = 5;

    private EmbeddedPulsarKafka cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new EmbeddedPulsarKafka();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void producesAndConsumesRecordsWithSequentialOffsets() throws Exception {
        List<Long> producedOffsets = produce();
        assertThat(producedOffsets).containsExactly(0L, 1L, 2L, 3L, 4L);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            TopicPartition partition = new TopicPartition(TOPIC, 0);
            cluster.pulsar().getAdminClient().topics()
                .createNonPartitionedTopic("persistent://public/default/native-pulsar-topic");
            assertThat(consumer.listTopics(Duration.ofSeconds(10)))
                .containsKey(TOPIC)
                .doesNotContainKey("native-pulsar-topic");
            consumer.assign(List.of(partition));

            assertThat(consumer.beginningOffsets(List.of(partition)).get(partition)).isEqualTo(0L);
            assertThat(consumer.endOffsets(List.of(partition)).get(partition)).isEqualTo((long) RECORD_COUNT);

            consumer.seekToBeginning(List.of(partition));
            List<String> keys = new ArrayList<>();
            List<String> values = new ArrayList<>();
            List<Long> offsets = new ArrayList<>();
            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(20);
            while (values.size() < RECORD_COUNT && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    keys.add(record.key());
                    values.add(record.value());
                    offsets.add(record.offset());
                }
            }
            assertThat(offsets).containsExactly(0L, 1L, 2L, 3L, 4L);
            assertThat(keys).containsExactly("k0", "k1", "k2", "k3", "k4");
            assertThat(values).containsExactly("v0", "v1", "v2", "v3", "v4");
        }

        PersistentTopic topic = (PersistentTopic) cluster.pulsar().getBrokerService()
            .getTopicReference("persistent://public/default/" + TOPIC).orElseThrow();
        assertThat(topic.getManagedLedger().getNumberOfEntries()).isPositive();
    }

    @Test
    void rejectsFetchOffsetsBeyondTheLogEnd() throws Exception {
        produce();
        Properties properties = consumerProperties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            TopicPartition partition = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(partition));
            consumer.seek(partition, RECORD_COUNT + 1L);

            assertThatThrownBy(() -> consumer.poll(Duration.ofSeconds(5)))
                .isInstanceOf(OffsetOutOfRangeException.class);
        }
    }

    @Test
    void producesSequentiallyWhenPulsarDeduplicationIsEnabled() throws Exception {
        cluster.close();
        cluster = new EmbeddedPulsarKafka(true);

        assertThat(produce()).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    void concurrentProducersRemainOrderedWhenPulsarDeduplicationIsEnabled() throws Exception {
        cluster.close();
        cluster = new EmbeddedPulsarKafka(true);
        int producers = 8;
        ExecutorService executor = Executors.newFixedThreadPool(producers);
        try {
            List<Future<Long>> futures = new ArrayList<>();
            for (int i = 0; i < producers; i++) {
                int record = i;
                futures.add(executor.submit(() -> {
                    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
                        return producer.send(new ProducerRecord<>(TOPIC, "ck" + record, "cv" + record))
                            .get(20, TimeUnit.SECONDS).offset();
                    }
                }));
            }
            List<Long> offsets = new ArrayList<>();
            for (Future<Long> future : futures) {
                offsets.add(future.get(30, TimeUnit.SECONDS));
            }
            Collections.sort(offsets);
            assertThat(offsets).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void migratesLegacyKafkaTopicFromEntryMarkerWithoutAdoptingNativeTopics() throws Exception {
        String legacyTopicName = "legacy-kpulse-topic";
        String pulsarTopicName = "persistent://public/default/" + legacyTopicName;
        PersistentTopic legacyTopic = (PersistentTopic) cluster.pulsar().getBrokerService()
            .getTopic(pulsarTopicName, true).get(10, TimeUnit.SECONDS).orElseThrow();
        MemoryRecords legacyRecords = MemoryRecords.withRecords(
            Compression.NONE, new SimpleRecord("legacy-value".getBytes(UTF_8)));
        new PartitionLog(legacyTopic, new KafkaEntryFormatter())
            .appendRecords(legacyRecords).get(10, TimeUnit.SECONDS);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            TopicPartition partition = new TopicPartition(legacyTopicName, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records.records(partition))
                .extracting(ConsumerRecord::value)
                .containsExactly("legacy-value");
            assertThat(consumer.listTopics(Duration.ofSeconds(10))).containsKey(legacyTopicName);
        }
    }

    @Test
    void discoversAdministrativelyMarkedLegacyTopicBeforeNamedAccess() throws Exception {
        String legacyTopicName = "cold-legacy-kpulse-topic";
        String pulsarTopicName = "persistent://public/default/" + legacyTopicName;
        PersistentTopic legacyTopic = (PersistentTopic) cluster.pulsar().getBrokerService()
            .getTopic(pulsarTopicName, true).get(10, TimeUnit.SECONDS).orElseThrow();
        MemoryRecords legacyRecords = MemoryRecords.withRecords(
            Compression.NONE, new SimpleRecord("cold-legacy-value".getBytes(UTF_8)));
        new PartitionLog(legacyTopic, new KafkaEntryFormatter())
            .appendRecords(legacyRecords).get(10, TimeUnit.SECONDS);
        cluster.pulsar().getAdminClient().topics()
            .updateProperties(pulsarTopicName, KafkaTopicManager.kafkaTopicProperties());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            assertThat(consumer.listTopics(Duration.ofSeconds(10))).containsKey(legacyTopicName);
        }
    }

    private List<Long> produce() throws Exception {
        List<Long> offsets = new ArrayList<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            for (int i = 0; i < RECORD_COUNT; i++) {
                RecordMetadata metadata = producer
                    .send(new ProducerRecord<>(TOPIC, "k" + i, "v" + i))
                    .get(20, TimeUnit.SECONDS);
                offsets.add(metadata.offset());
            }
        }
        return offsets;
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        return props;
    }

    private Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 20000);
        return props;
    }
}
