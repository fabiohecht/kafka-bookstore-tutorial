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

        KStream<String, Shipping> underwayShipments = shipping
                .filter((key, value) -> value.getStatus().equals("underway"))
                .selectKey((key, value) -> value.getPacket())
                .peek((k, v) -> log.info(" underway: {} {}", k, v.toString()));


        KStream<String, Shipping> deliveredShipments = shipping
                .filter((key, value) -> value.getStatus().equals("delivered"))
                .selectKey((key, value) -> value.getPacket())
                .peek((k, v) -> log.info(" delivered: {} {}", k, v.toString()));

        KStream<String, ShippingTime> timeToDeliver = deliveredShipments
                .join(underwayShipments,
                        (delivered, underway) -> ShippingTime.newBuilder()
                                .setCount(1)
                                .setTimeToDeliver(delivered.getTimestamp() - underway.getTimestamp())
                                .build(),
                        JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES).toMillis()));

        timeToDeliver
                .peek((k, v) -> log.info(" joined: {} {}", k, v.toString()))
                .groupBy((key, value) -> "fixed")
                .reduce(
                        (value1, value2) -> {
                            value1.setCount(value1.getCount() + value2.getCount());
                            value1.setTimeToDeliver(value1.getTimeToDeliver() + value2.getTimeToDeliver());
                            return value1;
                        }
                )
                .toStream()
                .peek((k, v) -> log.info(" write: {} {} avg={}s", k, v.toString(), v.getTimeToDeliver() / v.getCount()))
                .to(OUTPUT_TOPIC);


        // starts stream
        return new KafkaStreams(builder.build(), config);
    }

}