package sample.camel.consumer.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface DemoRepository extends MongoRepository<Demo, String> {

}
