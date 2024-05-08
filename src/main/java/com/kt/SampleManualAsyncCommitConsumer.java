package com.kt;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class SampleManualAsyncCommitConsumer {

  private final static String TOPIC_NAME = "consumer_client_test_001";
  private final static String BOOTSTRAP_SERVERS = "kbroker01:9092,kbroker02:9092,kbroker03:9092";
  private final static String GROUP_ID = "consumer-group-1";
  private final static String CLIENT_ID = "consumer-group-1-C3";

  public static void main(String[] args) {
    try (KafkaConsumer<String, String> consumer = getKafkaConsumer()) {
      consumer.subscribe(List.of(TOPIC_NAME));
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

        for (ConsumerRecord<String, String> record : records) {
          log.info("record:{}", record);
        }
        // 비동기 방식의 오프셋 커밋을 위해 commitAsync() 메서드를 호출하고, 커스텀 콜백을 전달
        consumer.commitAsync((offsets, e) -> {  // OffsetCommitCallback 클래스에 해당하는 람다식
          if (e != null) {
            log.error("Commit failed for offsets {}", offsets, e);
          } else {
            log.info("Commit succeeded");
          }
        });

      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  private static KafkaConsumer<String, String> getKafkaConsumer() {
    Properties configs = new Properties();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    configs.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID); // 클라이언트 ID 설정
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // 폴 크기 설정 가능
    //configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

    return new KafkaConsumer<>(configs);
  }
}
