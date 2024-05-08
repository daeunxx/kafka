package com.kt;

import java.util.Properties;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SampleMetaCheckProducer {

  private final static String TOPIC_NAME = "producer_client_test_001";
  private final static String BOOTSTRAP_SERVERS = "kbroker01:9092, kbroker02:9092, kbroker03:9092";

  private final static int msgCount = 2000;

  public static void main(String[] args) {
    try (KafkaProducer<String, String> producer = getKafkaProducer()) {
      ProducerRecord<String, String> record;
      long startTime, endTime, duration;
      startTime = System.nanoTime(); // 시작 시간 측정
      for (int i = 0; i < msgCount; i++) {
        record = new ProducerRecord<>(
            TOPIC_NAME, "broker operation check" + i,
            "this msg is for broker operation check test / " + i
        );
        Future<RecordMetadata> futureRecord = producer.send(record);  // Future 객체는 비동기 요청을 수행할 수 있음
        RecordMetadata metadata = futureRecord.get();                 // 하지만 여기에서는 동기 요청인 get() 메서드를 사용
        log.info("[CALLBACK metadata] {}", metadata.toString());
      }
      producer.flush();
      endTime = System.nanoTime(); // 종료 시간 측정
      duration = endTime - startTime; // 걸린 시간 계산
      log.info("Message sent in {} ms", duration / 1_000_000);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  static KafkaProducer<String, String> getKafkaProducer() {
    Properties configs = new Properties();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // 소요 시간 비교
    configs.put(ProducerConfig.ACKS_CONFIG, "1");
    //configs.put(ProducerConfig.ACKS_CONFIG, "0");
    return new KafkaProducer<>(configs);
  }
}
