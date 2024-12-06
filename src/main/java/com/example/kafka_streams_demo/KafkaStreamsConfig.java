package com.example.kafka_streams_demo;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamsConfig {

    @Bean
    public KafkaStreams kafkaStreams() {
        // Configuration to setup the streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());

        // Create the Stream
        StreamsBuilder builder = new StreamsBuilder();
        // Receive the "message" from producer as a Stream
        KStream<String, String> sourceStream = builder.stream("input-topic");

        // Transform the stream by converting values to uppercase
        KStream<String, String> processedStream = sourceStream.mapValues(value->value.toUpperCase());

        // Write the processed stream to the output topic
        processedStream.to("output-topic", Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.String()));

        // Build the Kafka Streams instance
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Start the stream
        streams.start();

        // Add shutdown hook to close streams on application shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }
}