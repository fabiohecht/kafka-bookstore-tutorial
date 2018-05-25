
# ipt-Confluent Workshop :: Kafka Bookstore Tutorial

## Overview

This tutorial is organized in the following sections.

 1. Use Case
 1. Architecture
 1. Running Platform
 1. Stream Data into Kafka
 1. Stream Data Transformation with KSQL
 1. Stream Data Transformation with Kafka Streams
 1. Stream Data out of Kafka

## Use Case

The Kafka Bookstore is an online shop specialized in books by or about Franz Kafka.

Coincidentally, it uses Apache Kafka as its messaging platform, due to its singular characteristics:

 - Data streaming in real time, supports also batching
 - Decouples microservices with event driven architecture
 - Integrated storage, which allows messages to be replayed in the future
 - Wide range of connectors to import and export data into and out of it (Kafka Connect)
 - Stream analysis and transformations with KSQL and Kafka Streams
 - Elasticity, scalability, robustness, and high availability
 - Cloud native
 - Open source with active community
 - Excellent tools and support from Confluent ;)
 - It’s awesome!

Since a couple of weeks now, a minimum viable product (MVP) has been released and it’s attracting a lot of attention.
The workflow is the following:

 1. Customer signs up/logs in
 1. Customer browses books
 1. Customer adds products to cart
 1. Customer places an order
 1. Customer pays (informed by third-party payment partner)
 1. Order is shipped (informed by shipping partner)
 1. Shipment is delivered or lost (informed by shipping partner)

Kafka is used as a data streaming platform, to decouple microservices and transform events.

## Architecture

As seen in the diagram below, there are source systems that produce events (i.e. write data to Kafka) and target systems
that will consume events. We will also use Kafka to transform and aggregate events.

![alt text](kafka-bookstore-architecture.png "Kafka Bookstore Tutorial Architecure")

### Source systems

The following systems will ingest data into Kafka.

#### CRM and Inventory Management System

We assume there is a CRM and Inventory Management System system that holds information regarding customers and products.
The systems that manage them use MySQL to keep their data.
All the needed information can be found in the “customer” and “book” tables.
As part of this tutorial, we'll use Kafka Connect and a CDC (change data capture) tool to stream all updates to those
tables in real time to Kafka.

#### Mocked Components

The following components have events mocked by a Java producer, which we will run as part of this tutorial.

##### Microservices logs

When a user views a product or adds an item in the shopping cart, an event is produced to the topic “interaction”.
When an order is placed, then the event is published to the topic “purchase”. The field “packet” is null at first, then
updated as soon as the order is shipped. But the order isn’t shipped before it’s paid for!

##### Payment API Endpoint

The Payment API Endpoint microservice knows when customers have paid for an order. Let's say it calls an API, or it is
called by a webhook, triggered by a file transfer, whatever. The point is that it produces a message in the “payment”
topic when it happens. For simplicity, all orders are paid in full.

##### Shipping Partner

Once the order is paid for, it is shipped. Once the Kafka Bookstore ships an item, it updates the “purchase” topic
with the packet id informed by the Shipping partner. Then, the Shipping Partner microservice is triggered by the
shipping company (let’s say at calls ans API) and produces a
message in the topic “shipping” with the status “underway”, “delivered”, or “lost”.

### Kafka Cluster

The Kafka Cluster groups all basic Kafka infrastructure. It contain one Zookeeper instance, one Kafka Broker, and one
Schema Registry instance. It is where all Kafka topics are stored together with their metadata (schemas).

### Data Transformation

We will use both KSQL and Kafka Streams to consume streaming data from Kafka Topics, transform and aggregate it, and
write (produce) it to other Kafka topics.

### Web UIs

A user can interact with Kafka using command-line tools, APIs, or Web UIs. In this tutorial, the following web UIs are
available:

 - Confluent Control Center
 - Landoop Topics UI, Connect UI, Schema Registry UI

### Rest Proxy

Exposes a REST interface that can be used by streaming clients that do not dispose of a native client library.
We won't use it for this demo.

### Elasticsearch and Kibana

We will use Kafka Connect to write some data to Elasticsearch and visualize them using Kibana.

### Topic overview

The following Kafka Topics will be used in this tutorial:

 * Book
 * Customer
 * Interaction
 * Purchase
 * Payment
 * Shipping

## Running Platform

Your choice: Local docker images or use VirtualBox image.

If you have an outdated laptop (i.e. with less than 16 GB RAM), please have a look
[here](https://ipt.jiveon.com/docs/DOC-2169).

### Prerequisites

### Local

Please install:

* Docker and Docker Compose
* Git, Maven, and a Java IDE
* JDK 8

Then clone this repository: https://github.com/fabiohecht/kafka-bookstore-tutorial.git

### VirtualBox image

Please install Virtual Box and get the image from a USB stick (ask Fabio).
It's large, but it already has all the stuff in it.

### Get the platform started

Let's first make sure you have all last-minute changes. Open a terminal and type:

    cd IdeaProjects/kafka-bookstore-tutorial
    git pull

Now let's start all Docker containers. Open a terminal and type:

    cd docker-compose
    docker-compose up -d

To see which containers are running and their statuses:

    docker-compose ps

Hopefully they all have status Up.
It may take like a minute until all is running. You can see the logs of each container by typing:

    docker-compose logs [image-name]

On my machine, the elasticsearch image could not start, the error was:

    elasticsearch         | [2018-05-24T15:20:11,779][INFO ][o.e.b.BootstrapChecks    ] [nw0uKIe] bound or publishing to a non-loopback address, enforcing bootstrap checks
    elasticsearch         | ERROR: [1] bootstrap checks failed
    elasticsearch         | [1]: max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]

The fix is doing a:

    sudo sysctl -w vm.max_map_count=262144

You should be able to use a browser and see a few web UIs running. The Web UIs are useful to visualize some information.

 - Confluent Control Center: http://localhost:9021
 - Landoop Kafka Topics UI: http://localhost:8000
 - Landoop Confluent Schema Registry UI: http://localhost:8001
 - Landoop Kafka Connect UI 1: http://localhost:8003
 - Landoop Kafka Connect UI 2: http://localhost:8004
 - Kibana web UI: http://localhost:5601

There are also command line tools and APIs that are more powerful.
The Kafka command-line tools are included in most of the images.
For example, the kafka-connect image.
For example, to list all existing topics, you can use the Confluent Control Center, the Landoop Topics UI,
or via command line:

    docker-compose exec kafka-connect-cp kafka-topics --list --zookeeper zookeeper:2181

Only system topics are listed for the moment, but soon enough we will create our own topics.

## Stream Data into Kafka

We will use Kafka Connect to stream two MySQL tables to Kafka topics, and start a Java application that
mocks events.

### MySQL

A MySQL Server was started by Docker Compose and preloaded with mock data. You can check it out with:

    docker-compose exec mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD inventory'

From the MySQL prompt, explore the two tables:

    SHOW TABLES;
    SELECT * FROM book;
    SELECT * FROM customers;

When you are done, exit with ctrl+D.

### Stream MySQL changes into Kafka

The Docker Compose set of containers includes one for Kafka Connect with the Confluent Platform connectors present (covering JDBC, HDFS, S3, and Elasticsearch), and another for Kafka Connect with the Debezium MySQL CDC connector.

To configure Debezium, run the follow call to the Kafka Connect REST endpoint. There are two configurations you can use; run both and inspect the results to understand the differences. In practice you would only use one.

Flattened schema, current record state only:

    curl -i -X POST -H "Accept:application/json" \
        -H  "Content-Type:application/json" http://localhost:8083/connectors/ \
        -d '{
          "name": "mysql-source-inventory",
          "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": "mysql",
                "database.port": "3306",
                "database.user": "debezium",
                "database.password": "dbz",
                "database.server.id": "42",
                "database.server.name": "asgard",
                "database.history.kafka.bootstrap.servers": "kafka:29092",
                "database.history.kafka.topic": "dbhistory.demo" ,
                "include.schema.changes": "true",
                "transforms": "unwrap,InsertTopic,InsertSourceDetails",
                "transforms.unwrap.type": "io.debezium.transforms.UnwrapFromEnvelope",
                "transforms.InsertTopic.type":"org.apache.kafka.connect.transforms.InsertField$Value",
                "transforms.InsertTopic.topic.field":"messagetopic",
                "transforms.InsertSourceDetails.type":"org.apache.kafka.connect.transforms.InsertField$Value",
                "transforms.InsertSourceDetails.static.field":"messagesource",
                "transforms.InsertSourceDetails.static.value":"Debezium CDC from MySQL on asgard"
                }
        }'

For interest, you can also create this one, but it is for interest only and not necessary for the rest of the execise. It puts the data in Kafka with a nested schema, before/after record state & binlog metadata.

    curl -i -X POST -H "Accept:application/json" \
        -H  "Content-Type:application/json" http://localhost:8083/connectors/ \
        -d '{
          "name": "mysql-source-inventory-raw",
          "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": "mysql",
                "database.port": "3306",
                "database.user": "debezium",
                "database.password": "dbz",
                "database.server.id": "44",
                "database.server.name": "asgard",
                "database.history.kafka.bootstrap.servers": "kafka:29092",
                "database.history.kafka.topic": "dbhistory.demo.raw" ,
                "include.schema.changes": "true",
                "transforms": "addTopicSuffix",
                "transforms.addTopicSuffix.type":"org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.addTopicSuffix.regex":"(.*)",
                "transforms.addTopicSuffix.replacement":"$1-raw"
                }
        }'

Check that imported data looks ok. Via command line we use the kafka-connect-cp container just because it has the command
line tools installed):

    docker-compose exec kafka-connect-cp kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic asgard.inventory.book --from-beginning --property schema.registry.url=http://schema-registry:8081

    docker-compose exec kafka-connect-cp kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic asgard.inventory.customers --from-beginning --property schema.registry.url=http://schema-registry:8081

Press CTRL-C to exit.

### Start Java Mock Producer

Let's start the Java Mock event producer. All of this can be done from within IntelliJ Idea, which is contained in the VM.
You can of course use your favorite IDE.
Let's first compile the project with Maven. On the right-hand side of IntelliJ's window, under "Maven Projects",
"Lifecycle", double click "clean", then double click "compile". Then:

 * On the project navigator (left part of the screen), navigate to src, main, java/...producer.
 * Open the class MockProducer. Have a look at how we produce events to Kafka by calling producer.send(). It uses the Kafka Producer API.
 * Run the MockProducer -- right click file name and "Run 'MockProducer.main()'.
 * If all goes well, we are producing mock events to Kafka!

Like before, you can use the kafka-avro-console-consumer or Landoop's UI to see the events coming in your topics.
Keep the mock event producer running in the background.

In real life, we could picture for example a Java Spring Boot microservice that exposes an API that, when called by the
Shipping company, produces the event.

#### If you want to dig in deeper

For more information about the data schemas used, have a look at src/main/resources/avro/Schemas.avsc. These are defined
in Avro format, which is fairly human-readable (Json). The latest spec can be found at http://avro.apache.org/docs/1.8.2/spec.html.
When the project is compiled with Maven, the Java classes for the data model are generated (in package import ch.ipt.handson.model),
as specified in the pom.xml file under &lt;buld>&lt;plugins>.

## Stream Data Transformation with KSQL

KSQL is a new product from Confluent officially released in March 2018. It offers a SQL-like query language to inspect
and transform data inside Kafka.

Launch the KSQL cli:

    docker-compose exec ksql-cli ksql http://ksql-server:8088

If you get an error that the server is not running, start it:

    docker-compose up ksql-server

### Explore topics

    SHOW TOPICS;
    PRINT 'interaction' FROM BEGINNING;

Press ctrl-C to cancel

If the Java producer is running, then run:

    PRINT 'interaction';

to see a live feed of messages being added to the topic (note the ommission of `FROM BEGINNING`)

### Register stream and tables

    CREATE TABLE BOOK with (kafka_topic='asgard.inventory.book', VALUE_FORMAT='AVRO', key='bookId');
    CREATE TABLE CUSTOMER with (kafka_topic='asgard.inventory.customers', VALUE_FORMAT='AVRO', key='Id');
    CREATE TABLE PURCHASE with (kafka_topic='purchase', VALUE_FORMAT='AVRO', key='purchaseId');
    CREATE TABLE PAYMENT with (kafka_topic='payment', VALUE_FORMAT='AVRO', key='transactionId');
    CREATE STREAM SHIPPING with (kafka_topic='shipping', VALUE_FORMAT='AVRO');
    CREATE STREAM INTERACTION with (kafka_topic='interaction', VALUE_FORMAT='AVRO');
    CREATE STREAM PURCHASE_STREAM with (kafka_topic='purchase', VALUE_FORMAT='AVRO');
    CREATE STREAM PAYMENT_STREAM with (kafka_topic='payment', VALUE_FORMAT='AVRO');

    CREATE STREAM CUSTOMER_SRC with (kafka_topic='asgard.inventory.customers', VALUE_FORMAT='AVRO');
    CREATE STREAM CUSTOMER_REKEY AS SELECT * FROM CUSTOMER_SRC PARTITION BY ID;
    CREATE TABLE CUSTOMER with (kafka_topic='CUSTOMER_REKEY', VALUE_FORMAT='AVRO', KEY='ID');

### Explore objects

    describe interaction;

    SET 'auto.offset.reset' = 'earliest';
    SELECT * FROM INTERACTION LIMIT 5;

### How many views have there been per category, per 30 second window?

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


### For each purchase, add customer details

Sample Purchase data:

    5/17/18 8:09:42 PM UTC, 60a7ba13-af98-4e98-ba0f-d45e5e83923c, {"purchaseId": "60a7ba13-af98-4e98-ba0f-d45e5e83923c", "customerId": 52, "bookIds": ["2tTdnAEACAAJ", "zBskDwAAQBAJ", "FbPQPwAACAAJ", "kHnrswEACAAJ", "Fr7vCgAAQBAJ", "ym9rAgAAQBAJ", "0JYeJp1F99cC", "IkAzf9IRVpIC", "KdDqtq_CemwC", "beYRAAAAQBAJ", "d0RcAAAAMAAJ"], "packet": null, "totalAmount": 28901}

Sample Customer data:

    5/17/18 2:54:39 PM UTC, 85, {"customerId": 85, "firstname": "Cordey", "lastname": "Targett", "email": "ctargett2c@mediafire.com", "street": "Rusk", "number": "00", "zip": "854", "city": "Frankfurt am Main", "country": "DE"}

Query:

    ksql> SELECT P.purchaseId, p.totalAmount, c.first_name + ' ' + c.last_name as full_name, c.email, c.city, c.country FROM PURCHASE_STREAM p LEFT JOIN CUSTOMER c on p.customerId=c.id LIMIT 1;
    f5d4e42f-c82f-4228-940e-19d6ddbf622c | 1500 | Cordey Targett | ctargett2c@mediafire.com | Frankfurt am Main | DE
    LIMIT reached for the partition.
    Query terminated

Persist this enriched stream, for external use and subsequent processing

    CREATE STREAM Purchase_enriched AS SELECT P.purchaseId, p.totalAmount, c.first_name + ' ' + c.last_name as full_name, c.email, c.city, c.country FROM PURCHASE_STREAM p LEFT JOIN CUSTOMER c on p.customerId=c.id;

### What's the geographical distribution by city of orders placed by 30 minute window?

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

### Trigger an event if a purchase is made over a given value

    CREATE STREAM BIG_PURCHASE AS SELECT * FROM PURCHASE_STREAM WHERE totalAmount>30000;

The resulting Kafka topic could be used to drive fraud checks, automatically hold orders, etc.

### Analytics: views per book

    ksql> select b.title, count(*) as view_count from interaction i left join book b on i.Id = b.bookId where i.event='view' group by b.title;
    Fremdheit in Kafkas Werken und Kafkas Wirkung auf die persische moderne Literatur | 34
    'Vor dem Gesetz' - Jacques Derridas Kafka-Lektüre | 23
    In der Strafkolonie | 59
    Kafka und Prag | 72

### What the total value of payments, per 5 second window?

(https://github.com/confluentinc/ksql/issues/430)

    CREATE STREAM PAYMENT2 AS SELECT 1 AS DUMMY,AMOUNT FROM PAYMENT_STREAM;
    CREATE TABLE TOTAL_PAYMENTS_BY_5SEC AS SELECT DUMMY, SUM(AMOUNT) AS TOTAL_AMOUNT FROM PAYMENT2 WINDOW TUMBLING (SIZE 5 SECONDS) GROUP BY DUMMY;
    SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') , TOTAL_AMOUNT FROM TOTAL_PAYMENTS_BY_5SEC;
    2018-05-17 21:36:35 | 26750
    2018-05-17 21:36:30 | 44200
    2018-05-17 21:36:50 | 25199
    2018-05-17 21:36:55 | 42051
    2018-05-17 21:37:00 | 10700

### KSQL reference

* https://www.confluent.io/product/ksql/
* [KSQL docs][3]
* [KSQL syntax reference][4]
* [KSQL Quickstart tutorial][5]
* [KSQL video tutorials][6]

  [3]: https://docs.confluent.io/current/ksql/docs/index.html#ksql-home
  [4]: https://docs.confluent.io/current/ksql/docs/syntax-reference.html
  [5]: https://docs.confluent.io/current/quickstart/ce-quickstart.html
  [6]: https://www.youtube.com/playlist?list=PLa7VYi0yPIH2eX8q3mPpZAn3qCS1eDX8W

## Stream data Transformation with Kafka Streams

A Kafka Streams application is a normal Java app, which makes it profit from Java's power and tool support.
Most Kafka Streams applications both read and write data to Kafka topics, though external systems can also be involved,
after all, the data is in a Java application.

While Kafka Streams is much more mature and powerful than KSQL, it does require a bit of experience with lambda
expressions and the Kafka Streams API.

The goal of this section is that you get a feeling about the basics of Kafka Streams and try out a few examples.

### Average shipping time

In this example, we calculate the average time the shipping partner takes to deliver ur orders.
We work with one input topic "shipping", correlating the timestamp of the record with status "underway" with the one
with status "delivered".

 * Open the file ch.ipt.handson.streams.ShipmentTimeStreamsApp
 * Read the comments in the code and try to make sense of it
 * Run the application (right click on the filename, select Run...)
 * Watch the app output (logs) and what's written to the topic (shipping-times)
 * The average shipping time calculated is roughly 5s, which matches the MEAN_DELIVERY_TIME_SECONDS constant defined in the MockProducer.
 * You can stop the app when you are done

### Outstanding Payments

This Kafka Streams Application outputs in real time which purchases have not already been paid, together with the
total amount to be received by the Kafka Bookstore.

 * Open the file ch.ipt.handson.streams.PaymentsOutstandingStreamsApp
 * Read the comments in the code try to make sense of it
 * Run the application (right click on the filename, select Run...)
 * Watch the app output (logs) and what's written to the output topic
 * You can stop the app when you are done if you want

### More information

More information about Kafka Streams can be found at
[Confluent](https://docs.confluent.io/current/streams/index.html), [Kafka](https://kafka.apache.org/documentation/streams/)
and [here](https://kafka.apache.org/11/javadoc/org/apache/kafka/streams/package-summary.html) is some good old Javadoc.

## Stream data out of Kafka

We'll use Kafka Connect again, but this time to stream data out of Kafka to Elasticsearch.
Then, we'll visualize the data in Kibana.

### Kafka Connect to Elasticsearch

Elasticsearch is a popular document storage that has powerful full-text search capabilities.
It is often used for text queries, analytics and as an key-value store.
For the key-value store use case, it supports using keys from Kafka messages as document ids in Elasticsearch and
provides configurations ensuring that updates to a key are written to Elasticsearch in order.
Elasticsearch’s idempotent write semantics guarantees exactly once delivery.

It if often used with Kibana, an open source visualization plugin to create real time dashboards.

The Elasticsearch connector is installed with Confluent Open Source by default. Create a mapping template:

    curl -XPUT "http://localhost:9200/_template/kafkaconnect/" -H 'Content-Type: application/json' -d'{"index_patterns":"*","settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"_default_":{"dynamic_templates":[{"dates":{"match":"EXTRACT_TS","mapping":{"type":"date"}}},{"non_analysed_string_template":{"match":"*","match_mapping_type":"string","mapping":{"type":"keyword"}}}]}}}'

Create a connector that streams the enriched purchase/customer data to Elasticsearch:

    curl -X "POST" "http://localhost:18083/connectors/" \
         -H "Content-Type: application/json" \
         -d '{
      "name": "es_sink_PURCHASE_ENRICHED",
      "config": {
        "topics": "PURCHASE_ENRICHED",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "key.ignore": "true",
        "schema.ignore": "false",
        "type.name": "type.name=kafkaconnect",
        "topic.index.map": "PURCHASE_ENRICHED:purchase_enriched",
        "connection.url": "http://elasticsearch:9200",
        "transforms": "ExtractTimestamp",
        "transforms.ExtractTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.ExtractTimestamp.timestamp.field" : "EXTRACT_TS"
      }
    }'

Create a connector that streams the Outstanding Payments stream to Elasticsearch


    curl -X "POST" "http://localhost:18083/connectors/" \
         -H "Content-Type: application/json" \
         -d '{
      "name": "outstanding_payments_elastic",
      "config": {
                      "topics": "outstanding-amount",
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
                      "connection.url": "http://elasticsearch:9200",
                      "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
                      "value.converter.schema.registry.url":"http://schema-registry:8081",
                      "type.name": "kafkaconnect",
        "key.ignore": "true",
        "schema.ignore": "false",
        "transforms": "ExtractTimestamp",
        "transforms.ExtractTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.ExtractTimestamp.timestamp.field" : "EXTRACT_TS"
      }
    }'

You can use the Kafka Connect API or Landoop to check the status of the connectors, for example with:

    http://localhost:18083/connectors
    http://localhost:18083/connectors/outstanding_payments_elastic/tasks/0/status


### Visualize data in Kibana

Check in Kibana that the data is coming in: http://localhost:5601. Hint: Create an index on outstanding-amount.
The details are left as an exercise to the reader ;)

Now you are free to play around in Kibana, write new transformations, or stream new data to Elastic.