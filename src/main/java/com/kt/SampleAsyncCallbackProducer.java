package com.kt;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SampleAsyncCallbackProducer {

  private final static String TOPIC_NAME = "producer_client_test_001";
  private final static String BOOTSTRAP_SERVERS = "kbroker01:9092,kbroker02:9092,kbroker03:9092";

  public static void main(String[] args) {
    try (KafkaProducer<String, String> producer = getKafkaProducer()) {
      ProducerRecord<String, String> record;
      long startTime, endTime, duration;
      startTime = System.nanoTime(); // 시작 시간 측정
      for (int i = 0; i < 800_000; i++) {
        record = new ProducerRecord<>(
            TOPIC_NAME, "async_callback_checker " + i, "this msg is for async callback test / " + i
        );
        // 비동기 콜백 interface 메서드 onCompletion 을 구현
        // 전송 완료 시 콜백 함수 onCompletion 이 호출됨
        // 별도 클래스 없이 인라인 override 및 람다식 사용 가능
        producer.send(record, new ProducerCallback());
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
    // 소요 시간 비교 (의미 없음) - 비동시 콜백 사용시에는 ACKS-CONFIG 설정 프로듀서 성능에 영향X
    //configs.put(ProducerConfig.ACKS_CONFIG, "1");
    //configs.put(ProducerConfig.ACKS_CONFIG, "0");
    return new KafkaProducer<>(configs);
  }
}
