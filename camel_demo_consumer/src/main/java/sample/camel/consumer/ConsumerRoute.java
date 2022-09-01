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
package sample.camel.consumer;

import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.DataFormat;
import org.springframework.stereotype.Component;
import sample.camel.consumer.kafka.KafkaMessage;
import sample.camel.consumer.mongo.Demo;
import sample.camel.consumer.mongo.DemoRepository;

@Slf4j
@Component
public class ConsumerRoute extends RouteBuilder {

  private static final String TIMESTAMP_KEY = "timestamp";

  @Resource
  private DemoRepository demoRepository;

  @Override
  public void configure() {
    from("kafka:{{spring.kafka.topic.demo}}?brokers={{spring.kafka.bootstrap-servers}}"
        + "&groupId={{camel.springboot.name}}"
        + "&keyDeserializer=org.apache.kafka.common.serialization.StringDeserializer"
        + "&valueDeserializer=sample.camel.consumer.kafka.JSONDeserializer"
        + "&autoOffsetReset=earliest"
        + "&additionalProperties.deserializeClass=sample.camel.consumer.kafka.KafkaMessage")
        .split().body(KafkaMessage.class, message -> {
          log.info("consume kafka message: {}", message);
          return message.getBody();
        })
        .choice()
          .when(exchange -> {
            Demo body = exchange.getMessage().getBody(Demo.class);
            return body.getGroup() > 0;
          }).marshal().xstream().convertBodyTo(String.class).to("log:info")
        .otherwise()
          .marshal().json().convertBodyTo(String.class).to("log:info");

  }

}
