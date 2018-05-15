package ch.ipt.handson.flat;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.List;

public class CustomerProducer {
    public static final String TOPIC_CUSTOMER = "customer-flat";

    static private Producer producer;

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
        producer = new KafkaProducer(GlobalConfiguration.getProducerCOnfig());
    }
}