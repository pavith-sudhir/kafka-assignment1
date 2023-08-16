package com.example.kafkaStreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class KafkaStreamApp2Exception {

    public static void main(final String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ex-queue-to-text-file");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> exQueueData = builder.stream("exceptions-q1", Consumed.with(Serdes.String(), Serdes.String()));

        // Print to the console
        exQueueData.print(Printed.toSysOut());

        // Write to a text file
        exQueueData.foreach((key, value) -> writeToFile(value));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); // Clean up state directory
        streams.start();
    }

    private static void writeToFile(String data) {
        try {
            FileWriter writer = new FileWriter("C:\\Apache-kafka\\kafka3\\invalidmessages.txt", true);
            writer.write(data + "\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
