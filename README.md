# ms-kafka-failure-simulation
This is a prototype service in where we have configured a main, retry and error topic. This service has an endpoint 
which publishes message to the main topic. If main topic fails, then it moves to retry topic. If retry topic fails 3 times it moves
to error topic.

However, the main topic would continue to consume hence this becomes more non-blocking for a consumer group.

This service has been built to support `ms-kafka-replay` you would need to clone [ms-kafka-replay](https://github.com/anojkunes/ms-kafka-replay).
You would need to run them both `ms-kafka-replay` and `ms-kafka-failure-simulation` and check the logs on what happens (This is where the true magic lies).

Use the docker-compose file provided in [kafka-replay-docs](https://github.com/anojkunes/kafka-replay-docs)
```bash
docker compose up -d
```

To access mongodb, please use the username and password provided in the docker-compose file:
```
      username: root
      password: rootpassword
```

# Dependencies
- Java 17
- Gradle
- Kafka

# Endpoints
Access endpoints from Open API: [here](http://localhost:8080/swagger-ui/)

# Logs

Check the logs to see if you are able too see any errors and what is happening when messages are being consumed
