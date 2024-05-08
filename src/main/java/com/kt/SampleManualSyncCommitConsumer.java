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
public class SampleManualSyncCommitConsumer {

  private final static String TOPIC_NAME = "consumer_client_test_001";
  private final static String BOOTSTRAP_SERVERS = "kbroker01:9092,kbroker02:9092,kbroker03:9092";
  private final static String GROUP_ID = "consumer-group-1";
  private final static String CLIENT_ID = "consumer-group-1-C2";

  public static void main(String[] args) {
    try (KafkaConsumer<String, String> consumer = getKafkaConsumer()){
      consumer.subscribe(List.of(TOPIC_NAME));
      int processedRecords = 0;
      int commitSize = 200;
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

        for (ConsumerRecord<String, String> record : records) {
          log.info("record:{}", record);
          processedRecords++;
        }
        // 1) polling한 메시지를 처리한 후 커밋하도록 설정하거나,
        //consumer.commitSync();
        // 2) 일정 갯수만큼 메시지를 처리할 때마다 커밋하도록 설정 가능
        if (processedRecords > commitSize ||
            // 커밋되지 않은 메시자가 있지만 새 메시지 수신이 멈췄을 때에도 커밋
            (processedRecords > 0 && records.isEmpty())) {
          consumer.commitSync();
          log.info("committed {} records", processedRecords);
          processedRecords = 0;
        }
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
    // 폴링 회차당 최대 크기 설정 가능
    //configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

    return new KafkaConsumer<>(configs);
  }
}
