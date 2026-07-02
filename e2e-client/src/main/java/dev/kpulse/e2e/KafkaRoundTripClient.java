package dev.kpulse.e2e;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * produce/consume CLI for the KIND e2e test. Records use keys/values "k&lt;i&gt;"/"v&lt;i&gt;", mirroring
 * {@code KafkaVerticalSliceIntegrationTest}. The consumer uses manual {@code assign()} to partition 0
 * rather than {@code subscribe()}: M1 has no consumer-group coordinator.
 */
public final class KafkaRoundTripClient {

    private KafkaRoundTripClient() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println(
                "usage: (produce <bootstrap> <topic> <count>)|(consume <bootstrap> <topic> <count> [timeoutMs])");
            System.exit(2);
            return;
        }

        String mode = args[0];
        String bootstrap = args[1];
        String topic = args[2];
        int count = Integer.parseInt(args[3]);

        switch (mode) {
            case "produce" -> produce(bootstrap, topic, count);
            case "consume" -> {
                long timeoutMs = args.length > 4 ? Long.parseLong(args[4]) : 30_000L;
                consume(bootstrap, topic, count, timeoutMs);
            }
            default -> {
                System.err.println("unknown mode: " + mode);
                System.exit(2);
            }
        }
    }

    private static void produce(String bootstrap, String topic, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=all rather than idempotence: enable.idempotence needs InitProducerId(22), which
        // kpulse only ships in M7. 60s budgets absorb cold-start topic/ledger creation on
        // constrained CI runners; no produce retry loop — re-sending would duplicate entries and
        // break the strict offset assertions.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String key = "k" + i;
                String value = "v" + i;
                RecordMetadata metadata =
                    producer.send(new ProducerRecord<>(topic, key, value)).get(60, TimeUnit.SECONDS);
                System.out.println("PRODUCED\t" + metadata.offset() + "\t" + key + "\t" + value);
            }
        }
    }

    private static void consume(String bootstrap, String topic, int count, long timeoutMs) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));

            int received = 0;
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (received < count && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    System.out.println(
                        "CONSUMED\t" + record.offset() + "\t" + record.key() + "\t" + record.value());
                    received++;
                }
            }
            if (received < count) {
                System.err.println("only received " + received + " of " + count + " records within " + timeoutMs + "ms");
                System.exit(1);
            }
        }
    }
}
