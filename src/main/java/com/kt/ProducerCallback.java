package com.kt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class ProducerCallback implements Callback {

  private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);
  private static final ConcurrentMap<Integer, AtomicInteger> partitionCounter = new ConcurrentHashMap<>() {{
    put(0, new AtomicInteger(0));
    put(1, new AtomicInteger(0));
    put(2, new AtomicInteger(0));
    put(-1, new AtomicInteger(0));
  }};

  @Override
  public void onCompletion(RecordMetadata metadata, Exception e) {
    if (e != null) {
      log.error(e.getMessage(), e);
    } else {
      // 발송 결과 메타데이터 콘솔에 출력
      log.info(metadata.toString());
      log.info(
          "[PARTITION COUNT] partition : {} / count : {}, sum :{}",
          metadata.partition(),
          partitionCounter.get(metadata.partition()).incrementAndGet(),
          partitionCounter.get(-1).incrementAndGet()
      );
    }
  }
}
