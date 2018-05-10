package ch.ipt.handson.producer;

import ch.ipt.handson.event.Book;
import ch.ipt.handson.event.Customer;
import ch.ipt.handson.event.WebsiteInteraction;
import com.github.javafaker.Faker;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class EventProducer {

    public static final String TOPIC_WEBSITE_INTERACTION = "interaction";


    public static final double VIEWS_PER_SECOND = 1.0;
    public static final double CART_ODDS = .1;
    public static final double ORDER_ODDS = .1;

    static private Producer producer;
    static private Faker faker;
    static private Random random = new Random();

    static final RateLimiter rateLimiter = RateLimiter.create(VIEWS_PER_SECOND);

    public static void main(String[] args) throws InterruptedException {
        setUpProducer();
        setUpData();
        produce();
    }

    private static void setUpData() {
        faker = new Faker(new Locale("de-CH"));
    }

    private static void produce() {
        while (true) {

            Book book = getRandomBook();
            Customer customer = getRandomCustomer();

            ProducerRecord<String, WebsiteInteraction> producerRecord =
                    new ProducerRecord<>(
                            TOPIC_WEBSITE_INTERACTION,
                            WebsiteInteraction.newBuilder()
                                    .setCustomerEmail(customer.getEmail())
                                    .setBook(book)
                                    .setSession(UUID.randomUUID().toString())
                                    .setEvent("view")
                                    .build());

            producer.send(producerRecord);
            System.out.println(producerRecord);

            rateLimiter.acquire();
        }


    }

    private static Customer getRandomCustomer() {
        return CustomersCollection.getCustomers().get(random.nextInt(CustomersCollection.getCustomers().size()));
    }

    private static Book getRandomBook() {
        return BooksCollection.getBooks().get(random.nextInt(BooksCollection.getBooks().size()));
    }

//        ArrayList<String> employees = new ArrayList<String>();
//        ArrayList<String> description = new ArrayList<String>();
//        ArrayList<String> expenses = new ArrayList<String>();
//
//        // Randoms for picking value out of ArrayLists
//        Random random_employee = new Random();
//        Random random_description = new Random();
//
//        // Max value of random generated number
//        int maxexpensevalue = 1000;
//
//        // Topic to produce to
//
//        // Time to sleep in ms
//        int timetosleep = 5000;

    // Create new property


//
//        Click
//        Producer<String, Click> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
//
//        // Fill ArrayLists with values
//        Collections.addAll(employees, "RKO","DAL","PGR","DAL","SHU","TIN","LKE","TSC","ASH","FHE");
//        Collections.addAll(description, "Mittagessen","Abendessen","Training","Bahn","Geschäftsauto","Wochenunterkunft","Flug","Hotel","BYO");
//
    // Do every x milliseconds


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
