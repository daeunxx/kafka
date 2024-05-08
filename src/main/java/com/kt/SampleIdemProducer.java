package com.kt;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SampleIdemProducer {

  private final static String TOPIC_NAME = "producer_client_test_001";
  private final static String BOOTSTRAP_SERVERS = "kbroker01:9092,kbroker02:9092,kbroker03:9092";
  private final static int msgCount = 1_000_000;
  private final static int transactionSize = 50_000;
  // TransactionSize 설정에 따라 성능 차이가 매우 크게 나타남
  //private final static int transactionSize = 5_000;
  public static void main(String[] args) {
    try (KafkaProducer<String, String> producer = getKafkaProducer()) {
      producer.initTransactions();
      ProducerRecord<String, String> record;
      boolean inTransaction = false;
      int transactionCount = 0;
      int firstInTransaction = 0;
      long startTime, endTime, duration;
      startTime = System.nanoTime(); // 시작 시간 측정
      for (int i = 0; i < msgCount; i++) {
        // i 가 transactionSize 로 나누어 떨어질 때 transaction 시작
        if (i % transactionSize == 0) {
          producer.beginTransaction();
          inTransaction = true;
          firstInTransaction = i;
        }
        record = new ProducerRecord<>(TOPIC_NAME, "[IDEMPOTENCE PRODUCER] " + i, "idempotence producer test / " + i);
        producer.send(record, new ProducerCallback());
        // transaction 사이즈 만큼 메시지 생성 후 transaction 종료
        if (i - firstInTransaction == transactionSize - 1) {
          inTransaction = false;
          producer.commitTransaction();
          transactionCount++;
          log.info("Transaction commit: {}", transactionCount);
        }
      }
      // transaction 열려 있는 경우 종료
      if (inTransaction) {
        producer.commitTransaction();
        transactionCount++;
        log.info("Transaction commit: {}", transactionCount);
      }
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

    // 멱등성 Producer 설정
    configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    configs.put(ProducerConfig.CLIENT_ID_CONFIG, "ClientUUID_0001");
    configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "Transactional_id_0001");
    configs.put(ProducerConfig.ACKS_CONFIG, "all");
    configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    // 재시도 횟수는 이미 기본값 Integer.MAX_VALUE 임
    //configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

    return new KafkaProducer<>(configs);
  }
}
