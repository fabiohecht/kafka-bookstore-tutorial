package ch.ipt.handson.producer;

import ch.ipt.handson.event.Customer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class CustomerProducer {
    public static final String TOPIC_CUSTOMER = "customer";

    static private Producer producer;
    static private CustomersCollection customersCollection;

    public static void main(String[] args) throws IOException {
        setUpProducer();
        parseResource();
    }

    private static void parseResource() throws IOException {

        List<Customer> customers = CustomersCollection.getCustomers();

        for (Customer customer : customers) {

            System.out.println(customer);

            producer.send(new ProducerRecord(TOPIC_CUSTOMER, customer.getCustomerId().toString(), customer));
        }
        System.out.println("Done producing customers.");

        producer.close();
    }

    private static void setUpProducer() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        producer = new KafkaProducer(properties);
    }
}