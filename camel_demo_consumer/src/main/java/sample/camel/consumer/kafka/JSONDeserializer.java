package sample.camel.consumer.kafka;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class JSONDeserializer<T> implements Deserializer<T> {

  private Type type;

  public JSONDeserializer() {
  }

  public JSONDeserializer(Type type) {
    this.type = type;
  }

  public JSONDeserializer(Class<T> clazz) {
    this.type = clazz;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String clazz = (String) configs.get("deserializeClass");
    try {
      this.type = Class.forName(clazz);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null || data.length == 0) {
      return null;
    }
    if (type == null) {
      return (T) JSON.parse(data);
    }
    if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) type;
      Type rawType = parameterizedType.getRawType();
      if (Collection.class.isAssignableFrom((Class<?>) rawType)) {
        return (T) JSON.parseArray(data, parameterizedType.getActualTypeArguments()[0]);
      }
    }
    return JSON.parseObject(data, type);
  }

}
