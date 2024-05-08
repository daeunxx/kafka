package com.kt;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class CustomPartitioner implements Partitioner {

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
      Cluster cluster) {
    if (keyBytes == null) {
      throw new InvalidRecordException("Need message key");
    }

    // 파티션 할당을 key 해싱에 맡기지 않고, 특정 메시지 패턴을 특정 partition 에 할당
    // 고속 처리용 0번 파티션
    if (((String) key).startsWith("[URGENT]")) {
      return 0;
    }
    // 연속 작업 순서 보장용 1번 파티션
    if (((String) key).contains("step") || ((String) key).contains("finish")) {
      return 1;
    }
    if (((String) value).contains("deviceId") ||
        ((String) value).contains("vehicleId") ||
        // 기기, 차량 등의 시계열 좌표 변화 추적용 2번 파티션
        ((String) value).contains("coordinate")) {
      return 2;
    }

    // 특정 조건에 빠지지 않은 경우 해시 함수 murmur2 와 mod 연산을 통한 균일 분배
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
