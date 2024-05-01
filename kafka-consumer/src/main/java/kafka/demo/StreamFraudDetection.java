package kafka.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;


public class StreamFraudDetection {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "stream-fraud-detection-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> transactions = 
            builder.stream("transaction_topic");

        // processing logic
        transactions.filter((key, value) -> {
            String[] values = value.split(",");
            double distanceFromHome = Double.parseDouble(values[0]);
            return distanceFromHome > 50.0; 
        }).to("flagged_transactions");

        // build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        //  shutdown hook to close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
