package kafka.demo;

import org.apache.kafka.clients.producer.*;
import java.io.FileReader;
import java.util.Properties;
import com.opencsv.CSVReader;
import java.io.IOException;

public class TransactionProducer {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java TransactionProducer <fileName.csv>");
            return;
        }
        String topicName = "transaction_topic";
        String fileName = args[0];

        // setup Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props); 

        try (// open CSV file
        CSVReader csvReader = new CSVReader(new FileReader(fileName))) {
            String[] nextRecord;

            // skip header row
            csvReader.readNext();

            while ((nextRecord = csvReader.readNext()) != null) {
                // construct the message as a string
                String message = String.join(",", nextRecord);

                // send the transaction record to Kafka
                producer.send(new ProducerRecord<>(topicName, null, message));
                System.out.println("Sent message: " + message);
            }
        }catch (IOException e) {
            System.out.println("Error reading the CSV file\n" + e);
        }
        producer.close();
    }
}
