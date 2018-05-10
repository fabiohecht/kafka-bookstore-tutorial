package ch.ipt.handson.producer;

import ch.ipt.handson.event.Book;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.stream.Collectors;

public class BookProducer {
    public static final String TOPIC_BOOK = "book";
    private static final String RESOURCE_FILE = "data/books-kafka.json";

    static private Producer producer;

    public static void main(String[] args) throws IOException {
        setUpProducer();
        parseResource();
    }

    private static void parseResource() throws IOException {
        ClassLoader classLoader = BookProducer.class.getClassLoader();
        File file = new File(classLoader.getResource(RESOURCE_FILE).getFile());

        JsonArray books = Json.parse(new FileReader(file)).asArray();

        for (JsonValue book : books) {
            JsonObject bookObject = book.asObject();
            JsonObject volumeInfo = bookObject.get("volumeInfo").asObject();

            String id = bookObject.get("id").asString();
            String title = volumeInfo.get("title").asString();

            JsonValue authorsObj = volumeInfo.get("authors");
            String authors = authorsObj == null ? null : authorsObj.asArray().values().stream().map(jsonValue -> jsonValue.asString()).collect(Collectors.joining(","));

            JsonValue categoriesObj = volumeInfo.get("categories");
            String categories = categoriesObj == null ? null : categoriesObj.asArray().values().stream().map(jsonValue -> jsonValue.asString()).collect(Collectors.joining(","));

            Integer price = null;
            JsonObject saleInfo = bookObject.get("saleInfo").asObject();
            if (saleInfo != null) {
                JsonValue listPrice = saleInfo.asObject().get("listPrice");
                if (listPrice != null) {
                    JsonValue amount = listPrice.asObject().get("amount");
                    if (amount != null) {
                        price = Math.round(amount.asFloat() * 100F);
                    }
                }
            }

            if (price == null) {
                price = 50 * title.charAt(0);
            }

            Book bookValue = Book.newBuilder()
                    .setId(id)
                    .setTitle(title)
                    .setAuthors(authors)
                    .setCategories(categories)
                    .setPrice(price)
                    .build();

            System.out.println(bookValue);

            producer.send(new ProducerRecord(TOPIC_BOOK, id, bookValue));
        }
        System.out.println("Done producing books.");
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