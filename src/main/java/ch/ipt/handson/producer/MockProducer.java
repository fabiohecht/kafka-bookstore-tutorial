package ch.ipt.handson.producer;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ch.ipt.handson.model.*;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MockProducer {

    //we'll produce to these topics
    public static final String TOPIC_WEBSITE_INTERACTION = "interaction";
    public static final String TOPIC_ORDER = "purchase";
    public static final String TOPIC_PAYMENT = "payment";
    public static final String TOPIC_SHIPPING = "shipping";

    //these settings control the probabilities that an event is produced
    //you don't need to change them, unless you want to
    static private Random random = new Random();
    public static final double VIEWS_PER_SECOND = 2.0;
    public static final double CART_ODDS = .5;
    public static final double ORDER_ODDS = .2;
    public static final int MEAN_PAYMENT_TIME_SECONDS = 5;
    public static final int STDEV_PAYMENT_TIME_SECONDS = 30;
    public static final int MEAN_SHIPPING_TIME_SECONDS = 5;
    public static final int STDEV_SHIPPING_TIME_SECONDS = 2;
    public static final int MEAN_UNDERWAY_TIME_SECONDS = 5;
    public static final int STDEV_UNDERWAY_TIME_SECONDS = 2;
    public static final int MEAN_DELIVERY_TIME_SECONDS = 5;
    public static final int STDEV_DELIVERY_TIME_SECONDS = 3;
    private static final double PACKET_LOST_ODDS = .2;

    //the kafka producer!
    static private Producer producer;

    static final Map<Customer, List<Book>> carts = new HashMap<>();

    static final RateLimiter rateLimiter = RateLimiter.create(VIEWS_PER_SECOND);

    static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    static volatile boolean running = true;

    public static void main(String[] args) {
        setUpProducer();
        addShutdownHook();
        produce();
    }

    private static void produce() {
        int limit = Integer.MAX_VALUE;
        while (running && limit-- > 0) {

            Book book = getRandomBook();
            Customer customer = getRandomCustomer();

            //produce a view event (interaction topic)
            ProducerRecord<String, WebsiteInteraction> viewRecord = getInteractionRecord(book, customer, "view");
            producer.send(viewRecord);
            System.out.println("iVIEW " + viewRecord);


            if (random.nextDouble() < CART_ODDS) {
                addToCart(customer, book);

                //produce a cart event (interaction topic)
                ProducerRecord<String, WebsiteInteraction> cartRecord = getInteractionRecord(book, customer, "cart");
                producer.send(cartRecord);
                System.out.println("iCART " + cartRecord);

                if (random.nextDouble() < ORDER_ODDS) {

                    //produce an order event (interaction topic)
                    ProducerRecord<String, WebsiteInteraction> orderInteractionRecord = getInteractionRecord(book, customer, "order");
                    producer.send(cartRecord);
                    System.out.println("iORDER " + orderInteractionRecord);

                    //produce an order event (purchase topic)
                    ProducerRecord<String, Purchase> orderRecord = getPurchaseRecord(customer);
                    producer.send(orderRecord);
                    System.out.println("ORDER " + orderRecord);

                    carts.remove(customer);

                    executor.schedule(() -> producePurchasePayment(orderRecord.value()), Math.round(randomGaussian(MEAN_PAYMENT_TIME_SECONDS, STDEV_PAYMENT_TIME_SECONDS)), TimeUnit.SECONDS);
                }
            }
            rateLimiter.acquire();
        }

    }

    private static void addShutdownHook() {
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;

            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }));
    }

    private static void producePurchaseShipping(Purchase order) {
        String packet = RandomStringUtils.random(10, true, true);
        order.setPacket(packet);

        //update purchase with packet id
        ProducerRecord<String, Purchase> orderRecordWithPacket = getPurchaseRecord(order);
        producer.send(orderRecordWithPacket);

        //post sends status "underway"
        executor.schedule(() -> produceShippingUnderway(packet), Math.round(randomGaussian(MEAN_UNDERWAY_TIME_SECONDS, STDEV_UNDERWAY_TIME_SECONDS)), TimeUnit.SECONDS);
    }

    private static void produceShippingUnderway(String packet) {
        ProducerRecord<String, Shipping> shippingRecord = getShippingRecord(packet, "underway");
        producer.send(shippingRecord);
        System.out.println("SHIP " + shippingRecord);

        //post sends status "delivered" or "lost"
        executor.schedule(() -> produceShippingDone(packet), Math.round(randomGaussian(MEAN_DELIVERY_TIME_SECONDS, STDEV_DELIVERY_TIME_SECONDS)), TimeUnit.SECONDS);
    }

    private static void produceShippingDone(String packet) {
        ProducerRecord<String, Shipping> shippingRecord = getShippingRecord(packet,
                random.nextDouble() < PACKET_LOST_ODDS ? "lost" : "delivered");
        producer.send(shippingRecord);
        System.out.println("SHIP " + shippingRecord);
    }

    private static ProducerRecord<String, Shipping> getShippingRecord(String packet, String status) {
        return new ProducerRecord<>(
                TOPIC_SHIPPING,
                packet,
                Shipping.newBuilder()
                        .setPacket(packet)
                        .setStatus(status)
                        .setTimestamp(new Date().getTime())
                        .build()
        );
    }

    private static void producePurchasePayment(Purchase order) {
        String transactionId = UUID.randomUUID().toString();
        ProducerRecord paymentRecord = new ProducerRecord<>(
                TOPIC_PAYMENT,
                transactionId,
                Payment.newBuilder()
                        .setReferenceNumber(order.getPurchaseId())
                        .setTransactionId(transactionId)
                        .setTimestamp(new Date().getTime())
                        .setAmount(order.getTotalAmount())
                        .build());
        producer.send(paymentRecord);
        System.out.println("PAY " + paymentRecord);

        executor.schedule(() -> producePurchaseShipping(order), Math.round(randomGaussian(MEAN_SHIPPING_TIME_SECONDS, STDEV_SHIPPING_TIME_SECONDS)), TimeUnit.SECONDS);
    }

    private static double randomGaussian(double mean, double standardDeviation) {
        return Math.max(0.0, random.nextGaussian() * standardDeviation + mean);
    }

    private static ProducerRecord<String, Purchase> getPurchaseRecord(Customer customer) {
        String id = UUID.randomUUID().toString();
        Purchase order = buildPurchase(id, customer, null, carts.get(customer));
        return getPurchaseRecord(order);
    }

    private static ProducerRecord<String, Purchase> getPurchaseRecord(Purchase order) {
        return new ProducerRecord<>(
                TOPIC_ORDER,
                order.getPurchaseId(),
                order);
    }

    private static Purchase buildPurchase(String id, Customer customer, String packet, List<Book> shoppingCart) {
        return Purchase.newBuilder()
                .setPurchaseId(id)
                .setCustomerId(customer.getCustomerId())
                .setBookIds(shoppingCart.stream().map(book -> book.getBookId()).collect(Collectors.toList()))
                .setPacket(packet)
                .setTotalAmount(shoppingCart.stream().map(book -> book.getPrice()).reduce((p0, p1) -> p0 + p1).orElse(0))
                .build();
    }

    private static void addToCart(Customer customer, Book book) {
        List<Book> cart = carts.get(customer);
        if (cart == null) {
            cart = new ArrayList<>();
            carts.put(customer, cart);
        }
        cart.add(book);
    }

    private static ProducerRecord<String, WebsiteInteraction> getInteractionRecord(Book book, Customer customer, String event) {
        return new ProducerRecord<>(
                TOPIC_WEBSITE_INTERACTION,
                UUID.randomUUID().toString(),
                WebsiteInteraction.newBuilder()
                        .setCustomerEmail(customer.getEmail())
                        .setId(book.getBookId())
                        .setTitle(book.getTitle())
                        .setAuthors(book.getAuthors())
                        .setCategories(book.getCategories())
                        .setPrice(book.getPrice())
                        .setEvent(event)
                        .build());
    }

    private static Customer getRandomCustomer() {
        return CustomersCollection.getCustomers().get(random.nextInt(CustomersCollection.getCustomers().size()));
    }

    private static Book getRandomBook() {
        return BooksCollection.getBooks().get(random.nextInt(BooksCollection.getBooks().size()));
    }
    private static void setUpProducer() {
        producer = new KafkaProducer(GlobalConfiguration.getProducerCOnfig());
    }

}
