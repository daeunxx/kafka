package com.kt;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class SampleShutdownHookConsumer {

  private final static String TOPIC_NAME = "consumer_client_test_001";
  private final static String BOOTSTRAP_SERVERS = "kbroker01:9092,kbroker02:9092,kbroker03:9092";
  private final static String GROUP_ID = "consumer-group-1";

  public static void main(String[] args) {
    try (KafkaConsumer<String, String> consumer = getKafkaConsumer()) {
      // 1) main thread reference 확보
      final Thread mainThread = Thread.currentThread();

      // 2) shutdown hook 등록
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        log.info("Interrupt 감지, consumer.wakeup() 호출");
        consumer.wakeup();

        // main thread 실행 허용을 위해 main thread 에 대한 join() 호출
        try {
          mainThread.join();
        } catch (InterruptedException e) {
          log.error(Arrays.toString(e.getStackTrace()));
        }
      }));

      // 3) 토픽 구독
      consumer.subscribe(List.of(TOPIC_NAME));
      // 4) 데이터 폴링
      while (true) {
        ConsumerRecords<String, String> records =consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {
          log.info("Key: " + record.key() + ", Value: " + record.value());
          log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        }
      }
    } catch (WakeupException e) {
      log.info("Consumer is starting to shut down");
    } catch (Exception e) {
      log.error("Unexpected exception in the consumer", e);
    } finally {
      log.info("Shutdown hook 을 사용해 컨슈머 graceful shutdown 처리 완료");
    }
  }
  private static KafkaConsumer<String, String> getKafkaConsumer() {
    Properties configs = new Properties();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(configs);
  }
}
