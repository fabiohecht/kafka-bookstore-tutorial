package streams;

import ch.ipt.handson.model.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/* For performance, we want one single request to return all the following data about a purchase:
  - purchase details
  - customer
  - books bought
  - payment status
  - shipping status
 Thus, we willt ransform the events to denormalized data so it can be quickly looked up without joins.

 So we use Kafka Streams to transform and write the events to a document store (Mongo)
 TODO or Elastic?
 */
public class PurchasesDenormalized extends KafkaStreamsApp {

    static final Logger log = LoggerFactory.getLogger(AmountOutstanding.class);

    private static final String INPUT_TOPIC_PURCHASE = "purchase";
    private static final String INPUT_TOPIC_BOOK = "book";
    private static final String INPUT_TOPIC_CUSTOMER = "customer";
    private static final String INPUT_TOPIC_PAYMENT = "payment";
    private static final String OUTPUT_TOPIC = "purchases-full";


    static public void main(String[] args) {
        initializeConfig();
        KafkaStreams streams = buildStream();
        startStream(streams);
    }

    private static KafkaStreams buildStream() {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Purchase> purchase = builder.stream(INPUT_TOPIC_PURCHASE);
        final GlobalKTable<String, Book> bookTable = builder.globalTable(INPUT_TOPIC_BOOK);
        final GlobalKTable<String, Book> customerTable = builder.globalTable(INPUT_TOPIC_CUSTOMER);
        //  final KStream<String, Payment> payment = builder.stream(INPUT_TOPIC_PAYMENT);

        //we could produce one event with customer, purchase, books,
        KTable<String, DenormalizedPurchase> denormalizedPurchaseKStream = purchase

                //we use flatMap to create one event per book purchased (needed because our join is one to many)
                .flatMap((key, value) -> {
                    List<KeyValue<String, Purchase>> ret = new ArrayList<>(value.getBookIds().size());
                    for (String bookId : value.getBookIds()) {
                        ret.add(KeyValue.pair(bookId, value));
                    }
                    return ret;
                })
                .join(bookTable,
                        (leftKey, purchase1) -> leftKey, /* derive a (potentially) new key by which to lookup against the table */
                        (purchase1, book) -> DenormalizedPurchase.newBuilder()
                                .setShoppingCart(
                                        ShoppingCart.newBuilder()
                                                .setBooks(Arrays.asList(book))
                                                .build()
                                )
                                .setPurchaseId(purchase1.getPurchaseId())
                                .setShipment(Shipping.newBuilder().build())
                                .setTotalAmount(purchase1.getTotalAmount())
                               // .build()
                )
                .groupByKey()
                .reduce((value1, value2) -> {
                    value1.getShoppingCart().getBooks().add(value2.getShoppingCart().getBooks().get(0));
                    return value1;
                })


                .mapValues((readOnlyKey, value) -> value.build());

        // starts stream
        return new KafkaStreams(builder.build(), config);
    }

}
