package sample.camel.consumer.kafka;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import sample.camel.consumer.mongo.Demo;

@Data
@AllArgsConstructor
public class KafkaMessage {

  private List<Demo> body;

}
