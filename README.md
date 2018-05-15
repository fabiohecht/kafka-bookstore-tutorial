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

- have a look at all components

Kafka topics UI: http://localhost:8000

Landoop Kafka Connect UI http://localhost:8000/#/


docker-compose up -d

docker ps

docker logs [name]

KSQL
----

docker exec -it dockercompose_ksql-cli_1 ksql http://ksql-server:8088


<code>
worked:

CREATE TABLE BOOK with (kafka_topic='book-flat', VALUE_FORMAT='AVRO', key='bookId');
CREATE TABLE CUSTOMER with (kafka_topic='customer-flat', VALUE_FORMAT='AVRO', key='customerId');
CREATE TABLE BOOKORDER with (kafka_topic='order-flat', VALUE_FORMAT='AVRO', key='orderId');
CREATE TABLE PAYMENT with (kafka_topic='payment-flat', VALUE_FORMAT='AVRO', key='transactionId');
CREATE STREAM SHIPPING with (kafka_topic='shipping-flat', VALUE_FORMAT='AVRO');


ksql> CREATE STREAM INTERACTION with (kafka_topic='interaction-flat', VALUE_FORMAT='AVRO');


CREATE TABLE pageviews_categories WITH (value_format='avro') AS \
SELECT categories , COUNT(*) AS num_views \
FROM INTERACTION \
WINDOW TUMBLING (size 30 second) \
WHERE event='view' \
GROUP BY categories \
HAVING COUNT(*) > 1;


**join orders with customer, books, shipping, and payments**

CREATE TABLE orders_full WITH (value_format='avro') AS \

SELECT BOOKORDER.orderId , payment.timestamp, payment.amount, shipping.timestamp, shipping.status \
FROM BOOKORDER \
left join payment ON payment.orderId = BOOKORDER.orderId \
left join shipping ON shipping.packet = BOOKORDER.packet;
> io.confluent.ksql.parser.exception.ParseFailedException
  Caused by: java.lang.NullPointerException

Breaking it down to find error cause:

SELECT * \
FROM BOOKORDER \
left join payment ON payment.orderId = BOOKORDER.orderId;
> java.lang.NullPointerException

SELECT * \
FROM BOOKORDER \
left join shipping ON shipping.packet = BOOKORDER.packet;
> Unsupported Join. Only stream-table joins are supported, but was io.confluent.ksql.planner.plan.StructuredDataSourceNode@473cd960-io.confluent.ksql.planner.plan.StructuredDataSourceNode@7fdb958



</code>


SELECT BOOKORDER.orderId, packet FROM BOOKORDER;

SELECT payment.orderId , payment.timestamp, payment.amount FROM payment;
