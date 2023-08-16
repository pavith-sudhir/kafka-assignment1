package com.example.kafkaStreams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class KafkaStreamApp1Main {

    public static void main(final String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payload-to-json-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("transactions-q1", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> validData = textLines
                .mapValues(KafkaStreamApp1Main::extractAndConvertPayload)
                .filter((key, value) -> !value.equals("{}"));

        KStream<String, String> debData = validData
                .filter((key, value) -> value.contains("transactionType\":\"debit"))
                .selectKey((key, value) -> "debit"); //to debit q

        KStream<String, String> credData = validData
                .filter((key, value) -> value.contains("transactionType\":\"credit"))
                .selectKey((key, value) -> "credit"); //to credit q

        KStream<String, String> invalidData = textLines
                .filter((key, value) -> {
                    String convertedValue = extractAndConvertPayload(value);
                    return convertedValue.equals("{}");
                }); // to exception-q


        debData.to("debit-q1", Produced.with(Serdes.String(), Serdes.String()));
        credData.to("credit-q1", Produced.with(Serdes.String(), Serdes.String()));
        invalidData.to("exceptions-q1", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
    }

    private static String extractAndConvertPayload(String message) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode rootNode = objectMapper.readTree(message);
            String payload = rootNode.get("payload").asText();
            return convertToJSON(payload);
        } catch (Exception e) {
            e.printStackTrace();
            return "{}"; // Default empty JSON
        }
    }

    private static String convertToJSON(String payload) {
        String[] parts = payload.split(",");
        if (parts.length != 8) {
            return "{}"; // Invalid data format
        }

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();

        try {
            jsonNode.put("transactionId", Integer.parseInt(parts[0]));
            jsonNode.put("firstName", parts[1]);
            jsonNode.put("secondName", parts[2]);
            jsonNode.put("accountType", parts[3]);
            jsonNode.put("accountNumber", Integer.parseInt(parts[4]));
            jsonNode.put("transactionType", parts[5]);
            jsonNode.put("amount", new BigDecimal(parts[6]));

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date transactionDate = dateFormat.parse(parts[7]);
            jsonNode.put("transactionDate", transactionDate.toString());

            return objectMapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            e.printStackTrace();
            return "{}"; // Default empty JSON
        }
    }
}
