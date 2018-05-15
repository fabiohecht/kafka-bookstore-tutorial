package ch.ipt.handson.flat;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class InteractionProducer {

    public static final String TOPIC_WEBSITE_INTERACTION = "interaction-flat";
    public static final String TOPIC_ORDER = "order-flat";
    public static final String TOPIC_PAYMENT = "payment-flat";
    public static final String TOPIC_SHIPPING = "shipping-flat";


    public static final double MEAN_VIEWS_PER_SECOND = 2.0;
    public static final double STDEV_VIEWS_PER_SECOND = 1.0;
    public static final double CART_ODDS = .5;
    public static final double ORDER_ODDS = .2;
    public static final int MEAN_PAYMENT_TIME_SECONDS = 60;
    public static final int STDEV_PAYMENT_TIME_SECONDS = 30;
    public static final int MEAN_SHIPPING_TIME_SECONDS = 5;
    public static final int STDEV_SHIPPING_TIME_SECONDS = 2;
    public static final int MEAN_UNDERWAY_TIME_SECONDS = 10;
    public static final int STDEV_UNDERWAY_TIME_SECONDS = 2;
    public static final int MEAN_DELIVERY_TIME_SECONDS = 30;
    public static final int STDEV_DELIVERY_TIME_SECONDS = 10;
    private static final double PACKET_LOST_ODDS = .2;

    static private Producer producer;
    static private Random random = new Random();
    static final Map<Customer, List<Book>> carts = new HashMap<>();

    static final RateLimiter rateLimiter = RateLimiter.create(randomGaussian(MEAN_VIEWS_PER_SECOND, STDEV_VIEWS_PER_SECOND));

    static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);


    public static void main(String[] args) throws InterruptedException {
        setUpProducer();
        produce();
    }

    private static void produce() {
        int limit = 10;
        while (limit-- > 0) {

            Book book = getRandomBook();
            Customer customer = getRandomCustomer();

            ProducerRecord<String, WebsiteInteraction> viewRecord = getInteractionRecord(book, customer, "view");
            producer.send(viewRecord);
            System.out.println("iVIEW " + viewRecord);


            if (random.nextDouble() < CART_ODDS) {
                addToCart(customer, book);

                ProducerRecord<String, WebsiteInteraction> cartRecord = getInteractionRecord(book, customer, "cart");
                producer.send(cartRecord);
                System.out.println("iCART " + cartRecord);

                if (random.nextDouble() < ORDER_ODDS) {

                    ProducerRecord<String, WebsiteInteraction> orderInteractionRecord = getInteractionRecord(book, customer, "cart");
                    producer.send(cartRecord);
                    System.out.println("iORDER " + orderInteractionRecord);

                    ProducerRecord<String, Order> orderRecord = getOrderRecord(customer);
                    producer.send(orderRecord);
                    System.out.println("ORDER " + orderRecord);

                    carts.remove(customer);

                    executor.schedule(() -> produceOrderPayment(orderRecord.value()), Math.round(randomGaussian(MEAN_PAYMENT_TIME_SECONDS, STDEV_PAYMENT_TIME_SECONDS)), TimeUnit.SECONDS);
                }
            }
            rateLimiter.acquire();
        }

    }

    private static void produceOrderShipping(Order order) {
        String packet = RandomStringUtils.random(10, true, true);
        order.setPacket(packet);

        //we update order with packet id
        ProducerRecord<String, Order> orderRecordWithPacket = getOrderRecord(order);
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

    private static void produceOrderPayment(Order order) {
        String transactionId = UUID.randomUUID().toString();
        ProducerRecord paymentRecord = new ProducerRecord<>(
                TOPIC_PAYMENT,
                transactionId,
                Payment.newBuilder()
                        .setOrder(order.getOrderId())
                        .setTransactionId(transactionId)
                        .setTimestamp(new Date().getTime())
                        .setAmount(order.getTotalAmount())
                        .build());
        producer.send(paymentRecord);
        System.out.println("PAY " + paymentRecord);

        executor.schedule(() -> produceOrderShipping(order), Math.round(randomGaussian(MEAN_SHIPPING_TIME_SECONDS, STDEV_SHIPPING_TIME_SECONDS)), TimeUnit.SECONDS);
    }

    private static double randomGaussian(double mean, double standardDeviation) {
        return random.nextGaussian() * standardDeviation + mean;
    }

    private static ProducerRecord<String, Order> getOrderRecord(Customer customer) {
        String id = UUID.randomUUID().toString();
        Order order = buildOrder(id, customer, null, carts.get(customer));
        return getOrderRecord(order);
    }

    private static ProducerRecord<String, Order> getOrderRecord(Order order) {
        return new ProducerRecord<>(
                TOPIC_ORDER,
                order.getOrderId(),
                order);
    }

    private static Order buildOrder(String id, Customer customer, String packet, List<Book> shoppingCart) {
        return Order.newBuilder()
                .setOrderId(id)
                .setFirstname(customer.getFirstname())
                .setLastname(customer.getLastname())
                .setCustomerId(customer.getCustomerId())
                .setEmail(customer.getEmail())
                .setStreet(customer.getStreet())
                .setNumber(customer.getNumber())
                .setZip(customer.getZip())
                .setCity(customer.getCity())
                .setCountry(customer.getCountry())
                .setBooks(new ArrayList<>())
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
                WebsiteInteraction.newBuilder()
                        .setCustomerEmail(customer.getEmail())
                        .setId(book.getId())
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
