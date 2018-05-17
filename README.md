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


== KSQL

=== Explore topics

    SHOW TOPICS;
    PRINT 'interaction' FROM BEGINNING;

Press ctrl-C to cancel

If the Java producer is running, then run:

    PRINT 'interaction'

to see a live feed of messages being added to the topic (note the ommission of `FROM BEGINNING`)



=== Register stream and tables

    CREATE TABLE BOOK with (kafka_topic='book', VALUE_FORMAT='AVRO', key='bookId');
    CREATE TABLE CUSTOMER with (kafka_topic='customer', VALUE_FORMAT='AVRO', key='customerId');
    CREATE TABLE PURCHASE with (kafka_topic='purchase', VALUE_FORMAT='AVRO', key='purchaseId');
    CREATE TABLE PAYMENT with (kafka_topic='payment', VALUE_FORMAT='AVRO', key='transactionId');
    CREATE STREAM SHIPPING with (kafka_topic='shipping', VALUE_FORMAT='AVRO');
    CREATE STREAM INTERACTION with (kafka_topic='interaction', VALUE_FORMAT='AVRO');
    CREATE STREAM PURCHASE_STREAM with (kafka_topic='purchase', VALUE_FORMAT='AVRO');
    CREATE STREAM PAYMENT_STREAM with (kafka_topic='payment', VALUE_FORMAT='AVRO');

=== Explore objects

    describe interaction;

    SET 'auto.offset.reset' = 'earliest';
    SELECT * FROM INTERACTION LIMIT 5;


=== How many views have there been per category, per 30 second window?

    CREATE TABLE pageviews_categories AS \
    SELECT categories , COUNT(*) AS num_views \
    FROM INTERACTION \
    WINDOW TUMBLING (size 30 second) \
    WHERE event='view' \
    GROUP BY categories;

    SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') , CATEGORIES, NUM_VIEWS FROM pageviews_categories;

    2018-05-17 14:57:30 | Kafka, Franz, 1883-1924 | 1
    2018-05-17 14:57:30 | Language Arts & Disciplines | 2
    2018-05-17 14:57:30 | History | 1
    2018-05-17 14:56:30 | Authors, Austrian | 4
    2018-05-17 14:57:00 | Education | 1
    2018-05-17 14:57:00 | Biography & Autobiography | 1
    2018-05-17 14:57:00 | Authors, Austrian | 4
    2018-05-17 14:57:00 | Grotesque in literature | 1

=== For each purchase, which books were bought?

Purchase data:

    5/17/18 8:09:42 PM UTC, 60a7ba13-af98-4e98-ba0f-d45e5e83923c, {"purchaseId": "60a7ba13-af98-4e98-ba0f-d45e5e83923c", "customerId": 52, "bookIds": ["2tTdnAEACAAJ", "zBskDwAAQBAJ", "FbPQPwAACAAJ", "kHnrswEACAAJ", "Fr7vCgAAQBAJ", "ym9rAgAAQBAJ", "0JYeJp1F99cC", "IkAzf9IRVpIC", "KdDqtq_CemwC", "beYRAAAAQBAJ", "d0RcAAAAMAAJ"], "packet": null, "totalAmount": 28901}

Book data:

    5/17/18 2:54:32 PM UTC, zOhEDgAAQBAJ, {"bookId": "zOhEDgAAQBAJ", "title": "Herz auf Eis", "authors": "Isabelle Autissier", "categories": "Fiction", "price": 2000}
    5/17/18 2:54:32 PM UTC, 587mAgAAQBAJ, {"bookId": "587mAgAAQBAJ", "title": "Frühstück bei Tiffany", "authors": "Truman Capote", "categories": "Fiction", "price": 1450}
    5/17/18 2:54:32 PM UTC, krU-DwAAQBAJ, {"bookId": "krU-DwAAQBAJ", "title": "Die Staatsräte", "authors": "Helmut Lethen", "categories": "History", "price": 2500}
    5/17/18 2:54:32 PM UTC, 4mE6XwAACAAJ, {"bookId": "4mE6XwAACAAJ", "title": "Auf der Suche nach der verlorenen Zeit", "authors": "Marcel Proust", "categories": null, "price": 3250}
    5/17/18 2:54:32 PM UTC, mqvzSAAACAAJ, {"bookId": "mqvzSAAACAAJ", "title": "Reise zum Mittelpunkt der Erde", "authors": "Jules Verne", "categories": null, "price": 4100}
    5/17/18 2:54:32 PM UTC, eanFlQEACAAJ, {"bookId": "eanFlQEACAAJ", "title": "In Swanns Welt", "authors": "Marcel Proust", "categories": null, "price": 3650}
    5/17/18 2:54:32 PM UTC, DHaQtAEACAAJ, {"bookId": "DHaQtAEACAAJ", "title": "Der mann ohne eigenschaften", "authors": "Robert Musil", "categories": null, "price": 3400}
    5/17/18 2:54:32 PM UTC, 6yxDtQEACAAJ, {"bookId": "6yxDtQEACAAJ", "title": "Das Judentum in der Musik : Was ist Deutsch ; Modern", "authors": "Richard Wagner", "categories": "Antisemitism", "price": 3400}
    5/17/18 2:54:32 PM UTC, XVn1jwEACAAJ, {"bookId": "XVn1jwEACAAJ", "title": "Der Prozess - Grossdruck", "authors": "Franz Kafka", "categories": null, "price": 3400}
    5/17/18 2:54:32 PM UTC, h_xsAgAAQBAJ, {"bookId": "h_xsAgAAQBAJ", "title": "Reise ans Ende der Nacht", "authors": "Louis-Ferdinand Céline", "categories": "Fiction", "price": 1300}

Can't be done in KSQL: would need to explode the nested bookIds object in order to do the join.

    ksql> select p.purchaseid, b.title from purchase_stream p left join book b on p.bookid=b.bookid;
     Line: 1, Col: 81 : Invalid join criteria (P.BOOKID = B.BOOKID). Key for P is not set correctly.

--> Do an example of Kafka Streams code here ?

=== For each purchase, add customer details

Sample Purchase data:

    5/17/18 8:09:42 PM UTC, 60a7ba13-af98-4e98-ba0f-d45e5e83923c, {"purchaseId": "60a7ba13-af98-4e98-ba0f-d45e5e83923c", "customerId": 52, "bookIds": ["2tTdnAEACAAJ", "zBskDwAAQBAJ", "FbPQPwAACAAJ", "kHnrswEACAAJ", "Fr7vCgAAQBAJ", "ym9rAgAAQBAJ", "0JYeJp1F99cC", "IkAzf9IRVpIC", "KdDqtq_CemwC", "beYRAAAAQBAJ", "d0RcAAAAMAAJ"], "packet": null, "totalAmount": 28901}

Sample Customer data:

    5/17/18 2:54:39 PM UTC, 85, {"customerId": 85, "firstname": "Cordey", "lastname": "Targett", "email": "ctargett2c@mediafire.com", "street": "Rusk", "number": "00", "zip": "854", "city": "Frankfurt am Main", "country": "DE"}

Query:

    ksql> SELECT P.purchaseId, p.totalAmount, c.firstname + ' ' + c.lastname as full_name, c.email, c.city, c.country FROM PURCHASE_STREAM p LEFT JOIN CUSTOMER c on p.customerId=c.customerId LIMIT 1;
    f5d4e42f-c82f-4228-940e-19d6ddbf622c | 1500 | Cordey Targett | ctargett2c@mediafire.com | Frankfurt am Main | DE
    LIMIT reached for the partition.
    Query terminated

Persist this enriched stream, for external use and subsequent processing

    CREATE STREAM Purchase_enriched AS SELECT P.purchaseId, p.totalAmount, c.firstname + ' ' + c.lastname as full_name, c.email, c.city, c.country FROM PURCHASE_STREAM p LEFT JOIN CUSTOMER c on p.customerId=c.customerId;

=== What's the geographical distribution by city of orders placed by 30 minute window?

    ksql> SELECT city,COUNT(*) FROM Purchase_enriched WINDOW TUMBLING (SIZE 30 MINUTES) GROUP BY city;

    Limoges | 3
    Caen | 3
    Limoges | 4
    Brest | 4
    Düsseldorf | 7
    Armentières | 12
    Mayenne | 4
    London | 5

Persist it as a kafka topic:

    ksql> CREATE TABLE PURCHASE_COUNT_BY_CITY_PER_30MIN AS SELECT city,COUNT(*) as purchase_count FROM Purchase_enriched WINDOW TUMBLING (SIZE 30 MINUTES) GROUP BY city;

     Message
    ---------------------------
     Table created and running
    ---------------------------

Query it:

    ksql> SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') , CITY, PURCHASE_COUNT FROM PURCHASE_COUNT_BY_CITY_PER_30MIN;

    2018-05-17 20:30:00 | La Rochelle | 2
    2018-05-17 20:30:00 | Poitiers | 1
    2018-05-17 20:30:00 | Le Mans | 1

=== Trigger an event if a purchase is made over a given value

    CREATE STREAM BIG_PURCHASE AS SELECT * FROM PURCHASE_STREAM WHERE totalAmount>30000;

The resulting Kafka topic could be used to drive fraud checks, automatically hold orders, etc.

=== Denormalise orders with customer, books, shipping, and payments

Goal is to "sink connect" to mongo, so data can be queried by api client.

Can't be done in KSQL currently as requires stream-stream join.

--> Do an example of Kafka Streams code here.

Attempted KSQL:

```
CREATE TABLE orders_full WITH (value_format='avro') AS \

SELECT purchase.purchaseId , payment.timestamp, payment.amount, shipping.timestamp, shipping.status FROM purchase left join payment ON payment.referenceNumber = purchase.purchaseId left join shipping ON shipping.packet = purchase.packet;

io.confluent.ksql.parser.exception.ParseFailedException Caused by: java.lang.NullPointerException

Breaking it down to find error cause:

SELECT * FROM purchase left join payment ON payment.referenceNumber = purchase.purchaseId;

java.lang.NullPointerException

SELECT * FROM purchase left join shipping ON shipping.packet = purchase.packet;

Unsupported Join. Only stream-table joins are supported, but was io.confluent.ksql.planner.plan.StructuredDataSourceNode@473cd960-io.confluent.ksql.planner.plan.StructuredDataSourceNode@7fdb958
```

=== What the total value of payments, per 5 second window?

(https://github.com/confluentinc/ksql/issues/430)

    CREATE STREAM PAYMENT2 AS SELECT 1 AS DUMMY,AMOUNT FROM PAYMENT_STREAM;
    CREATE TABLE TOTAL_PAYMENTS_BY_5SEC AS SELECT DUMMY, SUM(AMOUNT) AS TOTAL_AMOUNT FROM PAYMENT2 WINDOW TUMBLING (SIZE 5 SECONDS) GROUP BY DUMMY;
    SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') , TOTAL_AMOUNT FROM TOTAL_PAYMENTS_BY_5SEC;
    2018-05-17 21:36:35 | 26750
    2018-05-17 21:36:30 | 44200
    2018-05-17 21:36:50 | 25199
    2018-05-17 21:36:55 | 42051
    2018-05-17 21:37:00 | 10700

=== Which payments have been received for which purchases?

This is a stream-stream join, and not yet supported in KSQL

--> Do an example of Kafka Streams code here?

=== Amount outstanding (i.e. amount ordered but not yet paid

This is a stream-stream join, and not yet supported in KSQL

--> Do an example of Kafka Streams code here?

**average time from ordered to paid, shipped, and received: where can we optimize?**

=== Analytics: views per book

ksql> select b.title, count(*) as view_count from interaction i left join book b on i.Id = b.bookId where i.event='view' group by b.title;
Fremdheit in Kafkas Werken und Kafkas Wirkung auf die persische moderne Literatur | 34
'Vor dem Gesetz' - Jacques Derridas Kafka-Lektüre | 23
In der Strafkolonie | 59
Kafka und Prag | 72

== KSQL reference

* https://www.confluent.io/product/ksql/
* [KSQL docs][3]
* [KSQL syntax reference][4]
* [KSQL Quickstart tutorial][5]
* [KSQL video tutorials][6]


  [3]: https://docs.confluent.io/current/ksql/docs/index.html#ksql-home
  [4]: https://docs.confluent.io/current/ksql/docs/syntax-reference.html
  [5]: https://docs.confluent.io/current/quickstart/ce-quickstart.html
  [6]: https://www.youtube.com/playlist?list=PLa7VYi0yPIH2eX8q3mPpZAn3qCS1eDX8W
