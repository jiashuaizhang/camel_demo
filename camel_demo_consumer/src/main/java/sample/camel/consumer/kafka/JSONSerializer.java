package sample.camel.consumer.kafka;

import com.alibaba.fastjson2.JSON;
import org.apache.kafka.common.serialization.Serializer;

public class JSONSerializer<T> implements Serializer<T> {

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null) {
      return null;
    }
    return JSON.toJSONBytes(data);
  }

}
