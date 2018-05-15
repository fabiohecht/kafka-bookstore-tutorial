# kafka-mock-event-producer
A Kafka Producer that produces random events.



<code>
worked:

ksql> CREATE TABLE BOOK with (kafka_topic='book', VALUE_FORMAT='AVRO', key='id');

ksql> CREATE TABLE CUSTOMER with (kafka_topic='customer', VALUE_FORMAT='AVRO', key='customerId');

ksql> CREATE TABLE PAYMENT with (kafka_topic='payment', VALUE_FORMAT='AVRO', key='transactionId');

ksql> CREATE STREAM SHIPPING with (kafka_topic='shipping', VALUE_FORMAT='AVRO');


ksql> CREATE TABLE ORDER_TABLE with (kafka_topic='order', VALUE_FORMAT='AVRO', key='orderId');
 Unable to verify the AVRO schema is compatible with KSQL. KSQL doesn't currently support Avro type: ch.ipt.handson.event.Customer 

ksql> CREATE STREAM INTERACTION with (kafka_topic='interaction', VALUE_FORMAT='AVRO');
ksql> CREATE STREAM INTERACTION with (kafka_topic='interaction', VALUE_FORMAT='AVRO');
 Unable to verify the AVRO schema is compatible with KSQL. KSQL doesn't currently support Avro type: ch.ipt.handson.event.Book 


</code>

ls
