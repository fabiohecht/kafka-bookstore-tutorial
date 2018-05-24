package ch.ipt.handson.streams;

import ch.ipt.handson.model.OutstandingPayments;
import ch.ipt.handson.model.Payment;
import ch.ipt.handson.model.Purchase;
import ch.ipt.handson.model.PurchasePayment;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * This Kafka Streams Application outputs in real time which purchases have not already been paid, together with the
 * total amount to be received by Bookstore.
 */
public class PaymentsOutstandingStreamsApp extends KafkaStreamsApp {
    static final Logger log = LoggerFactory.getLogger(PaymentsOutstandingStreamsApp.class);

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

        final KStream<String, Purchase> purchaseStream = builder.stream(INPUT_TOPIC_PURCHASE);
        final KStream<String, Payment> paymentStream = builder.stream(INPUT_TOPIC_PAYMENT);

        //we want to left (outer) join purchaseStream and paymentStream
        //on purchase.purchaseId and payment.referenceNumber
        //to know which purchases were paid and which weren't

        //we need to change the keys of paymentStream, as joins in Kafka Streams are always key based
        KStream<String, Payment> paymentRekeyed = paymentStream
                .selectKey((key, value) -> value.getReferenceNumber());

        purchaseStream
                //we concentrate ourselves in events that occur before shipping (packet==null means not shipped yet)
                .filter((key, value) -> value.getPacket() == null)

                //for debugging/demo
                .peek((k, v) -> log.info(" before join: {} {}", k, v.toString()))

                //here comes the join between the purchase and payment topics
                //we use an intermediate object PurchasePayment to hold the information about both events
                .leftJoin(
                        paymentRekeyed,
                        //each event joined will be transformed in a new event of type PurchasePayment
                        (purchase, payment) -> new PurchasePayment(purchase, payment),
                        //in Kafka Streams, all joins between 2 streams are window-based
                        JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES).toMillis()))
                .peek((k, v) -> log.info(" after join: {} {}", k, v.toString()))

                //every aggregation needs a grouping function first
                //we want to aggregate all events together, so we group by a constant
                //in SQL this would be equivalent to a SELECT 'fixed', sum(...) ... GROUP BY 'fixed';
                .groupBy((key, value) -> "fixed")
                .aggregate(() -> OutstandingPayments.newBuilder().setOutstandingPaymentPurchases(new ArrayList<>()).build(),
                        (key, purchasePayment, aggregate) -> {
                            if (purchasePayment.getPayment() == null) {
                                //payment is outstanding, so we add
                                aggregate.getOutstandingPaymentPurchases().add(purchasePayment.getPurchase());
                                aggregate.setAmountOutstanding(aggregate.getAmountOutstanding() + purchasePayment.getPurchase().getTotalAmount());
                            } else {
                                //purchase has been paid up, hurray! not outstanding anymore!

                                //workaround .remove() not implemented by avro (i.e. getOutstandingPaymentPurchases().remove() throws exception)
                                List<Purchase> opp = new ArrayList<>(aggregate.getOutstandingPaymentPurchases());
                                opp.remove(purchasePayment.getPurchase());
                                aggregate.setOutstandingPaymentPurchases(opp);

                                aggregate.setAmountOutstanding(aggregate.getAmountOutstanding() - purchasePayment.getPayment().getAmount());
                            }

                            return aggregate;
                        })

                //output to topic
                .toStream()
                .peek((k, v) -> log.info(" write: {} {}", k, v.toString()))
                .to(OUTPUT_TOPIC);

        return new KafkaStreams(builder.build(), config);
    }

}
