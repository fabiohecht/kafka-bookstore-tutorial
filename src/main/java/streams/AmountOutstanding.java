package streams;

import ch.ipt.handson.model.OutstandingAmount;
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
import java.util.HashSet;
import java.util.Set;

/**
 * Amount outstanding (i.e. amount ordered but not yet paid)
 */
public class AmountOutstanding extends KafkaStreamsApp {
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

        final Set<String> outstandingPurchases = new HashSet<>();

        //we'll outer join purchase and payment, filter for payment null (outstanding) and add up the amounts
        //to prepare for the join, we need to change keys, as joins are always key based
        KStream<String, Payment> paymentRekeyed = payment
                .selectKey((key, value) -> value.getReferenceNumber());

        KTable<String, OutstandingAmount> joined = purchase
                .peek((k, v) -> log.info(" before join: {} {}", k, v.toString()))
                .outerJoin(
                        paymentRekeyed,
                        (purchase1, payment1) -> new PurchasePayment(purchase1, payment1),
                        JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES).toMillis()))
                .peek((k, v) -> log.info(" after join: {} {}", k, v.toString()))
                .mapValues((readOnlyKey, value) -> {
                    int amountOutstanding = 0;
                    if (value.getPayment() == null) {
                        //null payment = outstanding
                        if (!outstandingPurchases.contains(readOnlyKey)) {
                            amountOutstanding = value.getPurchase().getTotalAmount();
                            outstandingPurchases.add(readOnlyKey);
                        } else {
                            log.warn("was about to count twice {}", readOnlyKey);
                        }
                    } else {
                        //non-null payment = we subtract the paid amount from outstanding
                        //but only if we added it before
                        if (outstandingPurchases.contains(readOnlyKey)) {
                            amountOutstanding = -value.getPayment().getAmount();
                            outstandingPurchases.remove(readOnlyKey);
                        } else {
                            log.warn("payment received for non-outstanding purchase {}", readOnlyKey);
                        }
                    }
                    return OutstandingAmount.newBuilder()
                            .setAmountOutstanding(amountOutstanding)
                            .build();
                })

                .peek((k, v) -> log.info(" outstanding: {} {}", k, v.toString()))

                //add all outstanding$
                //.selectKey((key, value) -> "fixed")
                .groupByKey()
                .reduce((value1, value2) -> {
                            value1.setAmountOutstanding(value1.getAmountOutstanding() + value2.getAmountOutstanding());
                            return value1;
                        }
                );

        joined.toStream()
                .peek((k, v) -> log.info(" write: {} {}", k, v.toString()))
                .to(OUTPUT_TOPIC);


        // starts stream
        return new KafkaStreams(builder.build(), config);
    }

}
