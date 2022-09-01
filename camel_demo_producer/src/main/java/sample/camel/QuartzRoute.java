/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sample.camel;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.language.JsonPathExpression;
import org.apache.camel.support.ExpressionAdapter;
import org.springframework.stereotype.Component;
import sample.camel.kafka.KafkaMessage;
import sample.camel.mongo.Demo;
import sample.camel.mongo.DemoRepository;

@Slf4j
@Component
public class QuartzRoute extends RouteBuilder {

  private static final String TIMESTAMP_KEY = "timestamp";

  @Resource
  private DemoRepository demoRepository;

  @Override
  public void configure() {
    from("quartz://myGroup/myTimerName?cron=0/5+*+*+*+*+?")
        .setProperty(TIMESTAMP_KEY, () -> System.currentTimeMillis())
        .process(exchange -> {
          long timestamp = exchange.getProperty(TIMESTAMP_KEY, Long.class);
          Demo demo = Demo.builder().createTime(timestamp).description("created At " + timestamp)
              .group(new Random().nextInt(2)).build();
          Demo savedDemo = demoRepository.save(demo);
          log.info("demo saved: {}", savedDemo);
          exchange.getMessage().setBody(savedDemo.getId());
        })
        .transform().exchange(exchange -> {
          String demoId = exchange.getMessage().getBody(String.class);
          Demo demo = demoRepository.findById(demoId).get();
          log.info("demo got: {}", demo);
          return demo;
        })
        .aggregate(new JsonPathExpression("$.group"), (oldExchange, newExchange) -> {
          Demo newBody = newExchange.getIn().getBody(Demo.class);
          ArrayList<Demo> list = null;
          if (oldExchange == null) {
            list = new ArrayList<>();
            list.add(newBody);
            newExchange.getIn().setBody(list);
            return newExchange;
          } else {
            list = oldExchange.getIn().getBody(ArrayList.class);
            list.add(newBody);
            return oldExchange;
          }
        }).completionSize(3)
        .setHeader(KafkaConstants.KEY, new ExpressionAdapter() {
          @Override
          public Object evaluate(Exchange exchange) {
            List<Demo> demoList = exchange.getIn().getBody(List.class);
            return demoList.stream().map(Demo::getId).collect(Collectors.joining(","));
          }
        })
        .transform().exchange(exchange -> {
          List<Demo> body = exchange.getMessage().getBody(List.class);
          return new KafkaMessage(body);
        })
        .to("log:info")
        .to("kafka:{{spring.kafka.topic.demo}}?brokers={{spring.kafka.bootstrap-servers}}"
            + "&keySerializer=org.apache.kafka.common.serialization.StringSerializer"
            + "&valueSerializer=sample.camel.kafka.JSONSerializer");

  }

}
