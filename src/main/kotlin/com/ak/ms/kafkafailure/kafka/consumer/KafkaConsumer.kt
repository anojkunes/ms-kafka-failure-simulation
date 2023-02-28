package com.ak.ms.kafkafailure.kafka.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class KafkaConsumer(
  val objectMapper: ObjectMapper
) {

  private val LOG = KotlinLogging.logger {}

  @KafkaListener(topics = ["\${spring.kafka.topics[0].name}"], containerFactory = "kafkaListenerContainerFactory", groupId = "\${spring.kafka.consumer.group-id}")
  fun onEvent(@Payload event: String, @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
    LOG.info("Recieved message on $topic::$event and payload:$event")

    throw RuntimeException()
  }

  @KafkaListener(topics = ["\${spring.kafka.topics[0].name}_\${spring.kafka.consumer.group-id}_RETRY"], containerFactory = "kafkaRetryListenerContainerFactory", groupId = "\${spring.kafka.consumer.group-id}")
  fun onRetryEvent(@Payload event: String, @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
    LOG.info("Recieved message on $topic::$event and payload:$event")
    val test = objectMapper.readValue(event, Map::class.java)
    LOG.info("converted = $test")
    throw RuntimeException()
  }
}