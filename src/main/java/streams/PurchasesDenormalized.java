package streams;

import ch.ipt.handson.model.Payment;
import ch.ipt.handson.model.Purchase;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 We want to transform the events to denormalized data so it can be quickly looked up without joins
 So we use Kafka Streams to transform and write the events to a document store (Mongo)
 TODO or Elastic?
 */
public class PurchasesDenormalized extends AbstractStream {

    static final Logger log = LoggerFactory.getLogger(AmountOutstanding.class);

    private static final String INPUT_TOPIC_PURCHASE = "purchase";
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
        final KStream<String, Payment> payment = builder.stream(INPUT_TOPIC_PAYMENT);
  //todo implement

        // starts stream
        return new KafkaStreams(builder.build(), config);
    }

}
