package com.kt;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SampleCustomPartition {

  private final static String TOPIC_NAME = "producer_client_test_001";
  private final static String BOOTSTRAP_SERVERS = "kbroker01:9092, kbroker02:9092, kbroker03:9092";

  public static void main(String[] args) {
    // try-with-resource 패턴 적용 : `Partitioner extends Closeable`
    try (KafkaProducer<String, String> producer = getKafkaProducer()) {
      ProducerRecord<String, String> record;
      // Time Critical 한 작업이 있는 경우 다른 작업들과 분리, 파티션 0번을 고속 처리 전용 Track 으로 사용
      for (int i = 0; i < 5; i++) {
        record = new ProducerRecord<>(TOPIC_NAME, "[URGENT] Emergency alert " + i,
            "this urgent msg is going to partition No.0 /" + i);
        producer.send(record);
        producer.flush();
      }
      // Steps & Finish 처리를 1번 Partition 에서 전담
      boolean fin = false;
      for (int i = 0; i < 5; i++) {
        record = new ProducerRecord<>(TOPIC_NAME, "step" + i,
            "this step by step task is going to partition No.1 /" + i);
        producer.send(record);
        producer.flush();
        if (i == 4) {
          record = new ProducerRecord<>(TOPIC_NAME, "finished_well",
              "this is also going to partition No.1 /fin");
          producer.send(record);
          producer.flush();
        }
      }
      // 시계열 추적이 필요한 기기, 차량, 좌표 데이터 순차 처리에 2번 파티션 지정
      // 1차 신호
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "deviceId A is at City hall(1) Station / to partition No.2"));
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "vehicleId A is at coordinate (123,456) / to partition No.2"));
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "unknown is at coordinate (1, null) /to partition No.2"));
      // 2차 신호
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "deviceId A is at GangNam(2) Station / to partition No.2"));
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "vehicleId A is at coordinate (234,567) / to partition No.2"));
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "unknown is at coordinate (2, -) /to partition No.2"));
      // 3차 신호
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "deviceId A is at AnGuk(3) Station / to partition No.2"));
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "vehicleId A is at coordinate (345,678) / to partition No.2"));
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter",
          "unknown is at coordinate (3, NaN) /to partition No.2"));
      producer.flush();
    }
  }

  static KafkaProducer<String, String> getKafkaProducer() {
    Properties configs = new Properties();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
    return new KafkaProducer<>(configs);
  }
}
