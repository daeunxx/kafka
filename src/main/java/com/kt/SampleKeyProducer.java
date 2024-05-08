package com.kt;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SampleKeyProducer {
  private final static String TOPIC_NAME = "producer_client_test_001";
  private final static String BOOTSTRAP_SERVERS = "kbroker01:9092";

  public static void main(String[] args) {
    // 1) producer 생성
    KafkaProducer<String, String> producer = getKafkaProducer();
    // 2) 메시지 레코드 생성
    for (int i = 0; i < 10; i++) {
      String messageValue = "this msg is produced with record key" + i;
      String messageKey = "key" + i;
      // ################ Key 할당된 레코드 생성 시 Key 값에 따라 Partition 결정 ################
      ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);
      // 3) Record 전송 전 Partitioner 단계에 등록
      producer.send(record);  // 4) 배치 구성
      log.info("[Record Content] {}", record);  // Record Content 확인
    }
    // 5) Record Batch 를 Kafka Broker 로 전송 후 접속 종료
    producer.flush();
    producer.close();
  }

  private static KafkaProducer<String, String> getKafkaProducer() {
    Properties configs = new Properties();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(configs);
  }
}
