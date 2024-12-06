package com.example.kafka_streams_demo;



import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class KafkaController {


   private final KafkaStreams kafkaStreams;


   @Autowired
   public KafkaController(KafkaStreams kafkaStreams) {
       this.kafkaStreams = kafkaStreams;
   }


   @GetMapping("/count/{word}")
   public Long getWordCount(@PathVariable String word) {
       ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(
         StoreQueryParameters.fromNameAndType("word-count-store", QueryableStoreTypes.keyValueStore())
       );
       return counts.get(word);
   }
}
