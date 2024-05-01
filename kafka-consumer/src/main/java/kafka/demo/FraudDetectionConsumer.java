package kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class FraudDetectionConsumer {
    public static void main(String[] args) {
        String topicName = "transaction_topic";
        String flaggedTopic = "flagged_transactions";
        Properties props = new Properties();

        // configure the consumer
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "fraud-detection-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        // create the consumer using props.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // subscribe to the transaction_topic
        consumer.subscribe(Arrays.asList(topicName));

        // configure the producer to forward flagged transactions
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(producerProps);
    
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String[] values = record.value().split(",");

                    //  distance_from_home is at index 0 in the CSV
                    double distanceFromHome = Double.parseDouble(values[0]);

                    // basic fraud detection logic
                    if (distanceFromHome > 50.0) { // Threshold distance
                        System.out.println("Flagged transaction: " + record.value());
                        producer.send(new ProducerRecord<>(flaggedTopic, null, record.value()));
                    }
                }
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
