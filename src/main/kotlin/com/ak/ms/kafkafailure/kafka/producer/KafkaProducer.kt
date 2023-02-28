package com.ak.ms.kafkafailure.kafka.producer

import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders.KEY
import org.springframework.kafka.support.KafkaHeaders.TOPIC
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Service

@Service
class KafkaProducer(
  private val kafkaTemplate: KafkaTemplate<String, Any>,
  private val objectMapper: ObjectMapper
  ) {
  private val LOG = KotlinLogging.logger {}

    fun sendMessage(topic: String, key: String, data: Any) {
      val payload = objectMapper.writeValueAsString(data)
      LOG.info("converted payload: $payload")
      val message = MessageBuilder
        .withPayload(payload)
        .setHeader(TOPIC, topic)
        .setHeader(KEY, key)
        .build();
      kafkaTemplate.send(message)
    }

  fun sendMaintainingOrderMessage(topic: String, key: String, data: Any, headers: Map<String, Any> = emptyMap()) {
    val payload = objectMapper.writeValueAsString(data)
    LOG.info("converted payload: $payload")
    val messageBuilder = MessageBuilder
      .withPayload(payload)
      .setHeader(TOPIC, topic)
      .setHeader(KEY, key)

    if (headers.isNotEmpty()) {
      headers.forEach {
        messageBuilder.setHeader(it.key, it.value)
      }
    }

    kafkaTemplate.send(messageBuilder.build())
  }
}