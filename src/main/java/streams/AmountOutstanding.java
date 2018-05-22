package streams;

import ch.ipt.handson.model.Payment;
import ch.ipt.handson.model.Purchase;
import ch.ipt.handson.model.PurchasePayment;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Amount outstanding (i.e. amount ordered but not yet paid)
 */
public class AmountOutstanding extends AbstractStream {
    static final Logger log = LoggerFactory.getLogger(AmountOutstanding.class);

    private static final String INPUT_TOPIC_PURCHASE = "purchase";
    private static final String INPUT_TOPIC_PAYMENT = "payment";
    private static final String OUTPUT_TOPIC = "outstanding-amount";


    static public void main(String[] args) {
        initializeConfig();
        KafkaStreams streams = buildStream();
        startStream(streams);
    }

    private static KafkaStreams buildStream() {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Purchase> purchase = builder.stream(INPUT_TOPIC_PURCHASE);
        final KStream<String, Payment> payment = builder.stream(INPUT_TOPIC_PAYMENT);
        //final KTable<String, Book> employeeTable = builder.table(INPUT_TOPIC_EMPLOYEE, Materialized.with(stringSerde, stringSerde));

        //we'll outer join purchase and payment, filter for payment null (outstanding) and add up the amounts
        KStream<String, Payment> paymentRekeyed = payment.selectKey((key, value) -> value.getReferenceNumber());
        KTable<String, AmountOutstanding> joined = purchase
                .peek((k, v) -> log.info(" before join: {} {}", k, v.toString()))
                .outerJoin(
                        //to prepare for the join, we need to change keys, as joins are always key based
                        paymentRekeyed,
                        (purchase1, payment1) -> new PurchasePayment(purchase1, payment1),
                        JoinWindows.of(Duration.of(2, ChronoUnit.MINUTES).toMillis()))
                //.selectKey((key, purchasePayment) -> purchasePayment.getPayment() == null ? "outstanding" : "paid")
                //.filter((key, value) -> key.equals("outstanding"))
                .mapValues((readOnlyKey, value) -> AmountOutstanding.newBuilder()
                        .setPurchaseId(readOnlyKey)
                        .setAmountOutstanding(
                                (value.getPayment() != null ? value.getPayment().getAmount() : 0) - value.getPurchase().getTotalAmount())
                        .build())

                .peek((k, v) -> log.info(" outstanding: {} {}", k, v.toString()))

                .groupByKey()

                //add all outstanding
                .reduce((value1, value2) -> {
                    value1.setAmountOutstanding(value1.getAmountOutstanding() + value2.getAmountOutstanding());
                    return value1;
                });

//fixme this only sums, we also need to subtract when a payment is made

        joined.toStream()
                .peek((k, v) -> log.info(" write: {} {}", k, v.toString()))
                .to(OUTPUT_TOPIC);


        // starts stream
        return new KafkaStreams(builder.build(), config);
    }

}
