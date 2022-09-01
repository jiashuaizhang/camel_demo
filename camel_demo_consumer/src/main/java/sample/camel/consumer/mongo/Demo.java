package sample.camel.consumer.mongo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("demo")
public class Demo {

  @Id
  private String id;
  @CreatedDate
  private Long createTime;
  private String description;
  private Integer group;

}
