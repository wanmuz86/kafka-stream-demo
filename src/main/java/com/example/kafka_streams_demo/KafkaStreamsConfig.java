package com.example.kafka_streams_demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamsConfig {

    @Bean
    public KafkaStreams kafkaStreams() {
        // Configuration for Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams"); // Optional: Set a custom state directory

        // Create the StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Receive the "message" from producer as a Stream
        KStream<String, String> sourceStream = builder.stream("input-topic");

        // Scenario 1: Convert messages to uppercase
        KStream<String, String> processedStream = sourceStream.mapValues(value->value.toUpperCase());
        processedStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Scenario 2: Word count logic
        // Split sentences into words and transform to lowercase
        KStream<String, String> wordsStream = sourceStream.flatMapValues(value ->
                Arrays.asList(value.toLowerCase().split("\\W+")));

        // Group by each word and count occurrences
        KTable<String, Long> wordCounts = wordsStream
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("word-count-store"));

        // Output the word counts to the "word-counts-topic"
        wordCounts.toStream().to("word-counts-topic", Produced.with(Serdes.String(), Serdes.Long()));

        // Build the Kafka Streams instance
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Start the stream
        streams.start();

        // Add shutdown hook to close the streams on application termination
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }
}
