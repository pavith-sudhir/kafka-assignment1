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
import java.math.BigDecimal;
import java.util.Properties;

public class KafkaStreamApp4CreditWarning {

    public static void main(final String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "credit-suspicion-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> debitQueueData = builder.stream("credit-q3", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> filteredCreditData = debitQueueData.filter((key, value) -> isSuspicious(value));

        filteredCreditData.print(Printed.toSysOut());
        filteredCreditData.foreach((key, value) -> writeToFile(value));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
    }

    private static boolean isSuspicious(String value) {
        String[] parts = value.split(",");
        Float amount = new Float(parts[6].replaceAll("\"amount\":", "").replaceAll("\"", "").trim());
        return (amount > 1000.0);
    }

    private static void writeToFile(String data) {
        String[] parts = data.split(",");
        Integer accountNumber = new Integer(parts[4].replaceAll("\"accountNumber\":", "").replaceAll("\"", "").trim());
        String firstName = parts[1].replaceAll("\"firstName\":", "").replaceAll("\"", "").trim();
        String secondName = parts[2].replaceAll("\"secondName\":", "").replaceAll("\"", "").trim();
        Float amount = new Float(parts[6].replaceAll("\"amount\":", "").replaceAll("\"", "").trim());
        try {
            FileWriter writer = new FileWriter("C:\\Apache-kafka\\kafka3\\CreditWarnings1.txt", true);
            String message = String.format("Credit transaction for account %s for customer %s %s is suspicious with amount %s.",
                    accountNumber, firstName, secondName, amount);
            writer.write(message + "\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
