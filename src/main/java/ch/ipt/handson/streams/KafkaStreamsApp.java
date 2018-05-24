package ch.ipt.handson.streams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * I put some common boilerplate code here so we can focus on the Kafka Streams part on the other classes
 */
public class KafkaStreamsApp {
    static Properties config;

    static void initializeConfig() {
        config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        //Kafka always remembers what events the application already processed, which is great for failure recovery
        //but for trying things out we want to start processing all events from start (i.e. offset 0) every time we rerun the app
        //adding a random UUID to the application ID is rather unusual in real life
        //I did it for development reasons, so every time we run the application, we start consuming all topics from the start (offset 0)

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "FHE-streams-" + UUID.randomUUID());
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "Streams-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        // Enable record cache of size 10 MB.
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);

        // Set commit interval to 1 second
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

    }

    static void startStream(KafkaStreams streams) {
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
