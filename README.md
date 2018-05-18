# kafka-mock-event-producer
A Kafka Producer that produces random events.


Prerequisites
-------------

**Prerequisites:**

- Docker
    - `macOS <https://docs.docker.com/docker-for-mac/install/>`__
    - `All platforms <https://docs.docker.com/engine/installation/>`__
- `Git <https://git-scm.com/downloads>`__


Running Platform
----------------

Your choice: Local docker images (hopefully download time is OK) or use shared infrastructure in AWS.

Local
-----

- Docker and Docker Compose must be installed and configured with at least 4 GB of memory.
 - [Mac OS|https://docs.docker.com/docker-for-mac/install/]
 - Other OSs [https://docs.docker.com/engine/installation/]
- Git


in src/main/docker-compose



docker-compose up -d

docker ps

docker logs [name]

KSQL
----

docker exec -it dockercompose_ksql-cli_1 ksql http://ksql-server:8088
or
docker-compose exec ksql-cli ksql http://ksql-server:8088


<code>
worked:

show topics;


CREATE TABLE BOOK with (kafka_topic='book', VALUE_FORMAT='AVRO', key='bookId');
CREATE TABLE CUSTOMER with (kafka_topic='customer', VALUE_FORMAT='AVRO', key='customerId');
CREATE TABLE PURCHASE with (kafka_topic='purchase', VALUE_FORMAT='AVRO', key='purchaseId');
CREATE TABLE PAYMENT with (kafka_topic='payment', VALUE_FORMAT='AVRO', key='transactionId');
CREATE STREAM SHIPPING with (kafka_topic='shipping', VALUE_FORMAT='AVRO');
CREATE STREAM INTERACTION with (kafka_topic='interaction', VALUE_FORMAT='AVRO');



describe interaction; --etc


SET 'auto.offset.reset' = 'earliest';  


--I like this: https://www.confluent.io/blog/using-ksql-to-analyse-query-and-transform-data-in-kafka


CREATE STREAM PAYMENTST with (kafka_topic='payment', VALUE_FORMAT='AVRO', key='transactionId');
CREATE STREAM purchasest with (kafka_topic='purchase', VALUE_FORMAT='AVRO', key='purchaseId');


CREATE STREAM PAYMENTST_BY_REFVO AS SELECT * FROM PAYMENTST PARTITION BY referenceNumber;

SELECT * \
FROM purchasest \
left join PAYMENTST_BY_REFVO ON PAYMENTST_BY_REFVO.referenceNumber = purchasest.purchaseId;
> Unsupported Join. Only stream-table joins are supported,

CREATE TABLE PAYMENT_BY_REFNO AS SELECT * FROM PAYMENT PARTITION BY referenceNumber;
> line 1:56: mismatched input 'PARTITION' expecting ';'


SELECT * \
FROM purchasest \
left join PAYMENT ON PAYMENT.referenceNumber = purchasest.purchaseId;
> WORKS????? NO, payment is always null

SELECT book.* \
FROM purchasest \
left join BOOK ON BOOK.bookId = purchasest.bookIds;
> confirms payment is always null


--CREATE TABLE PAYMENT_BY_REFNO with (kafka_topic='payment', VALUE_FORMAT='AVRO', key='purchaseId') PARTITION BY referenceNumber;






**views per category, 30sec window**
CREATE TABLE pageviews_categories WITH (value_format='avro') AS \
SELECT categories , COUNT(*) AS num_views \
FROM INTERACTION \
WINDOW TUMBLING (size 30 second) \
WHERE event='view' \
GROUP BY categories \
HAVING COUNT(*) > 1;


**join orders with customer, books, shipping, and payments**

Goal is to "sink connect" to mongo, so data can be queried by api client.

CREATE TABLE orders_full WITH (value_format='avro') AS \

SELECT purchase.purchaseId , payment.timestamp, payment.amount, shipping.timestamp, shipping.status \
FROM purchase \
left join payment ON payment.referenceNumber = purchase.purchaseId \
left join shipping ON shipping.packet = purchase.packet;
> io.confluent.ksql.parser.exception.ParseFailedException
  Caused by: java.lang.NullPointerException

Breaking it down to find error cause:

SELECT * \
FROM purchase \
left join payment ON payment.referenceNumber = purchase.purchaseId;
> java.lang.NullPointerException

SELECT * \
FROM purchase \
left join shipping ON shipping.packet = purchase.packet;
> Unsupported Join. Only stream-table joins are supported, but was io.confluent.ksql.planner.plan.StructuredDataSourceNode@473cd960-io.confluent.ksql.planner.plan.StructuredDataSourceNode@7fdb958



</code>


SELECT purchase.purchaseId, packet FROM purchase;

SELECT payment.referenceNumber , payment.timestamp, payment.amount FROM payment;

**all payments**

select sum(payment.amount) \
FROM payment \
WINDOW TUMBLING (size 5 second) \
GROUP BY *;
> io.confluent.ksql.parser.exception.ParseFailedException
  Caused by: java.lang.NullPointerException

select orderId, sum(amount) \
FROM payment_st \
WINDOW TUMBLING (size 5 second) \
GROUP BY orderId;
> finally something works! But per order SUM() makes no sense (we only have one payment per order)

select payment_st.orderId, purchase.* \
FROM payment_st \
LEFT JOIN purchase ON purchase.purchaseId = payment_st.orderId;
> join does not work, i.e. purchase is always null


**amount to receive (ordered but not yet paid)**

**average time from ordered to paid, shipped, and received: where can we optimize?** 


**analytics: views per book**
select book.title, count(*) from interaction left join book on interaction.bookId = book.bookId group by book.title;
> Line: 1, Col: 100 : Invalid join criteria (INTERACTION.BOOKID = BOOK.BOOKID). Key for INTERACTION is not set correctly. 
  is problem null keys for interaction?


**average ticket: should be easy** 



**some reference**

Available KSQL statements:

CREATE STREAM
CREATE TABLE
CREATE STREAM AS SELECT
CREATE TABLE AS SELECT
DESCRIBE
EXPLAIN
DROP STREAM
DROP TABLE
PRINT
SELECT
SHOW TOPICS
SHOW STREAMS
SHOW TABLES
SHOW QUERIES
SHOW PROPERTIES
TERMINATE

https://docs.confluent.io/current/ksql/docs/syntax-reference.html#id12


---

SELECT purchase.purchaseId , payment.timestamp, payment.amount \
FROM purchase \
left outer join payment ON payment.referenceNumber = purchase.purchaseId;

SELECT purchase.purchaseId , paymentst.timestamp, paymentst.amount \
FROM paymentst  \
right outer join purchase ON paymentst.referenceNumber = purchase.purchaseId;



SELECT purchase.purchaseId , payment.timestamp, payment.amount \
FROM purchase \
left outer join payment ON payment.referenceNumber = purchase.purchaseId;



CREATE STREAM interaction_by_bookid AS SELECT * FROM interaction PARTITION BY bookId;
select book.title, count(*) from interaction_by_bookid left join book on interaction_by_bookid.bookId = book.bookId group by book.title;

---

try top5 sold books by revenue (ideally join with payments to exclude unpaid orders + join with book to get author name)

CREATE STREAM purchasest with (kafka_topic='purchase', VALUE_FORMAT='AVRO', key='purchaseId');

SELECT bookIds, TOPK(totalAmount, 5) \
FROM purchasest \
WINDOW TUMBLING (SIZE 1 HOUR) \
GROUP BY bookIds;

