package streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class Streams extends AbstractStream {
    static final Logger log = LoggerFactory.getLogger(Streams.class);

    private static final String INPUT_TOPIC_INTERACTION = "interaction-avro-1";
    private static final String OUTPUT_TOPIC_TOP_VIEWED = "most-viewed";

    private static final String INPUT_TOPIC_ORDER = "order";

    private static Properties config = new Properties();

    static public void main(String[] args) {
        initializeConfig();
        startStream();
    }


    //ideas:

    //For each purchase, which books were bought?

    //Denormalise orders with customer, books, shipping, and payments

    //top 5 sold books by revenue (ideally join with payments to exclude unpaid orders + join with book to get author name)

    //What the total value of payments, per 5 second window?


    //Which payments have been received for which purchases?


    //average time from ordered to paid, shipped, and received: where can we optimize?**


    private static void startStream() {
//
//        log.info("starting kafka streams");
//
//        final StreamsBuilder builder = new StreamsBuilder();
//
//        final KStream<String, WebsiteInteraction> interactionKStream = builder.stream(INPUT_TOPIC_INTERACTION);
//        //final KTable<String, Book> employeeTable = builder.table(INPUT_TOPIC_EMPLOYEE, Materialized.with(stringSerde, stringSerde));
//
//        KStream<Book, BookStat> newKeyed =
//                interactionKStream
//
//                        // for demo/debugging purposes, output what has come in through the KStream (does not change stream)
//                        .peek((k, v) -> log.debug(" new interaction: {}", v.toString()))
//
//                        .map((key, value) -> KeyValue.pair(
//                                value.getBook(),
//                                BookStat.newBuilder()
//                                        .setBook(value.getBook())
//                                        .setViews(value.getEvent().equals("view") ? 1 : 0)
//                                        .setAdds(value.getEvent().equals("cart") ? 1 : 0)
//                                        .setOrders(value.getEvent().equals("order") ? 1 : 0)
//                                        .build())
//                        )
//
//                        // for demo/debugging purposes, output what has come in through the KStream (does not change stream)
//                        .peek((k, v) -> log.debug(" mapped: {} {}", k, v));
//
//        KGroupedStream<Book, BookStat> grouped = newKeyed
//                .groupByKey();
//
//        KTable<Book, BookStat> bookCounts = grouped.reduce((value1, value2) -> {
//            value1.setViews(value1.getViews() + value2.getViews());
//            value1.setAdds(value1.getAdds() + value2.getAdds());
//            value1.setOrders(value1.getOrders() + value2.getOrders());
//            return value1;
//        });
//
//
////        bookCounts.toStream()
////                .peek((k, v) -> log.debug(" counted: {} {}", k, v))
////                .to("test-12");
////
//
//        // for demo/debugging purposes, output what we are writing to the KTable
//        final TopK<Book> top10Books = new TopK<>(3);
//
//        bookCounts.toStream()
//                .peek((k, v) -> log.debug(" before filter: {}", v))
//                //if it made it into top K
//                .filter((book, bookStat) -> top10Books.add(bookStat.getViews(), book))
//                //update rankings
//                .peek((k, v) -> log.debug(" after filter: {}", v))
////                .peek((k, v) -> log.debug(" topK: {}", top10Books.getTopK().entrySet().stream().map(e -> e.getKey() + ":" + e.getValue().getTitle()).collect(Collectors.joining(", "))))
//
//                .flatMap(
//                        (book, bookStat) -> {
//                            final int[] rank = {1};
//                            List<KeyValue<Ranking, Book>> result = new LinkedList<>();
////                            top10Books.getRankingFrom(bookStat.getViews())
////                                   .forEach((key, book1) -> result.add(KeyValue.pair(Ranking.newBuilder().setRank(rank[0]++).build(), book1)));
//                            return result;
//                        }
//
//                )
//                .peek((k, v) -> log.debug(" after flatMap: {} {}", k, v))
//
//                .to("top-3-viewed-books-7");
//
//        //                // output KTable to topic
////                .to(OUTPUT_TOPIC_TOP_VIEWED);
//
//        // starts stream
//        final KafkaStreams streamsContracts = new KafkaStreams(builder.build(), config);
//
//        streamsContracts.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
//            // can be used to examine the throwable/exception and perform an appropriate action!
//            throwable.printStackTrace();
//        });
//
//        streamsContracts.cleanUp();
//        streamsContracts.start();
//
//        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
//        Runtime.getRuntime().addShutdownHook(new Thread(streamsContracts::close));
    }
}

