package ch.ipt.handson.producer;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Properties;

public class BookProducer {
    public static final String TOPIC_BOOK = "book";
    private static final String RESOURCE_FILE = "data/books-kafka.json";


    public static void main(String[] args) throws IOException {
//        setUpProducer();
        parseResource();
    }

    private static void parseResource() throws IOException {
        ClassLoader classLoader = BookProducer.class.getClassLoader();
        File file = new File(classLoader.getResource(RESOURCE_FILE).getFile());

        JsonArray books = Json.parse(new FileReader(file)).asArray();

        for (JsonValue book : books) {
            JsonObject bookObject = book.asObject();
            JsonObject volumeInfo = bookObject.get("volumeInfo").asObject();
            System.out.println(
                    bookObject.get("id").asString() + " " +
                            volumeInfo.get("title").asString() + " " +
                            volumeInfo.get("authors").asString() + " " +
                            volumeInfo.get("categories").asString() + " " +
            );
        }
    }

    private static void setUpProducer() {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
    }
}
