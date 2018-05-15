package ch.ipt.handson.flat;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.List;

public class BookProducer {
    public static final String TOPIC_BOOK = "book-flat";

    static private Producer producer;
    static private BooksCollection booksCollection;

    public static void main(String[] args) throws IOException {
        setUpProducer();
        parseResource();
    }

    private static void parseResource() throws IOException {

        List<Book> books = BooksCollection.getBooks();

        for (Book book : books) {

            System.out.println(book);

            producer.send(new ProducerRecord(TOPIC_BOOK, book.getId(), book));
        }
        producer.close();

        System.out.println("Done producing books.");
    }

    private static void setUpProducer() {
        producer = new KafkaProducer(GlobalConfiguration.getProducerCOnfig());
    }
}