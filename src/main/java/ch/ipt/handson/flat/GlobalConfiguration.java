package ch.ipt.handson.flat;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class GlobalConfiguration {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081";
    public static final String ACKS = "1";
    public static final String RETRIES = "3";
    public static final String LINGER_MS = "1";

    public static Properties getProducerCOnfig() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfiguration.BOOTSTRAP_SERVERS);
        properties.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, GlobalConfiguration.SCHEMA_REGISTRY_URL);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        properties.setProperty(ProducerConfig.ACKS_CONFIG, GlobalConfiguration.ACKS);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, GlobalConfiguration.RETRIES);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, GlobalConfiguration.LINGER_MS);

        return properties;
    }
}
