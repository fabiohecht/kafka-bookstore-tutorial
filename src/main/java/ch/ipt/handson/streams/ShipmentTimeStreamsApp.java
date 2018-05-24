package ch.ipt.handson.streams;

import ch.ipt.handson.model.Shipping;
import ch.ipt.handson.model.ShippingTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * In this example, we calculate the average delivery time of our orders.
 */
public class ShipmentTimeStreamsApp extends KafkaStreamsApp {

    static final Logger log = LoggerFactory.getLogger(PaymentsOutstandingStreamsApp.class);

    private static final String INPUT_TOPIC_SHIPPING = "shipping";
    private static final String OUTPUT_TOPIC = "shipping-times";

    static public void main(String[] args) {
        initializeConfig();
        KafkaStreams streams = buildStream();
        startStream(streams);
    }

    private static KafkaStreams buildStream() {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Shipping> shipping = builder.stream(INPUT_TOPIC_SHIPPING);

        /*
         * OK, here is the plan. We will join two streams (underway and delivered).
         * In Kafka Streams, joins are always on event keys. So we change the keys to the packet ID.
         */

        // we create one stream with all "underway" packets
        KStream<String, Shipping> underwayShipments = shipping
                .filter((key, value) -> value.getStatus().equals("underway"))

                //we need to set the key to the packet ID, because in Kafka Streams, all joins are key-based
                .selectKey((key, value) -> value.getPacket())
                .peek((k, v) -> log.info(" underway: {} {}", k, v.toString()));

        // stream with all "delivered" packets
        KStream<String, Shipping> deliveredShipments = shipping
                .filter((key, value) -> value.getStatus().equals("delivered"))
                .selectKey((key, value) -> value.getPacket())
                .peek((k, v) -> log.info(" delivered: {} {}", k, v.toString()));

        // here comes the join
        KStream<String, ShippingTime> timeToDeliver = deliveredShipments
                .join(underwayShipments,
                        //each join produces one event with the calculated delivery time
                        //we'll use this info to calculate the average later
                        (delivered, underway) -> ShippingTime.newBuilder()
                                .setCount(1)
                                .setTimeToDeliver(delivered.getTimestamp() - underway.getTimestamp())
                                .build(),

                        //joins between 2 streams are always windowed
                        JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES).toMillis()));

        timeToDeliver
                .peek((k, v) -> log.info(" joined: {} {}", k, v.toString()))

                //all aggregations in Kafka Streams need a groping function (similar to GROUP BY in SQL)
                //since we want to aggregate all records, we group by a constant
                .groupBy((key, value) -> "fixed")
                //map-reduce style
                .reduce(
                        (shippingTime1, shippingTime2) -> {
                            shippingTime1.setCount(shippingTime1.getCount() + shippingTime2.getCount());
                            shippingTime1.setTimeToDeliver(shippingTime1.getTimeToDeliver() + shippingTime2.getTimeToDeliver());
                            return shippingTime1;
                        }
                )

                //the average can be calculated from the sum and count that we write to the topic
                .toStream()
                .peek((k, v) -> log.info(" write: {} {} avg={}s", k, v.toString(), v.getTimeToDeliver() / v.getCount()))
                .to(OUTPUT_TOPIC);

        // starts stream
        return new KafkaStreams(builder.build(), config);
    }

}