package com.ak.ms.kafkafailure.kafka.consumer

import com.ak.ms.kafkafailure.mongo.entity.State
import com.ak.ms.kafkafailure.mongo.repository.RetryStateMongoRepository
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class MaintainingOrderKafkaConsumer(
  override val retryStateMongoRepository: RetryStateMongoRepository
): AbstractMaintainingOrderKafkaHandler(retryStateMongoRepository) {

  private val LOG = KotlinLogging.logger {}

  @KafkaListener(topics = ["\${spring.kafka.topics[1].name}"], containerFactory = "maintainingOrderKafkaListenerContainerFactory", groupId = "\${spring.kafka.consumer.group-id}")
  fun onOrderEvent(@Payload event: ConsumerRecord<String, String>, @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
    LOG.info("Recieved message on $topic::$event and payload:${event.value()}")
    LOG.info("Original Position of Message found: [${event.partition()}, ${event.offset()}]")
    handleEvent(event.partition(), event.offset(), event, State.RETRYING)
  }

  @KafkaListener(topics = ["\${spring.kafka.topics[1].name}_\${spring.kafka.consumer.group-id}_RETRY"], containerFactory = "maintainingOrderKafkaRetryListenerContainerFactory", groupId = "\${spring.kafka.consumer.group-id}")
  fun onOrderRetryEvent(@Payload event: ConsumerRecord<String, String>, @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
    LOG.info("Recieved message on $topic::$event and payload:${event.value()}")
    val header = event.headers().find { it.key() == "RETRYING_REFERENCE_ID" }?.value()?.let { String(it).split(":") } ?: throw RuntimeException("Header RETRYING_REFERENCE_ID was not found")
    LOG.info("Header found: $header")
    val originalPartition = header[0].toInt()
    val originalOffset = header[1].toLong()
    handleEvent(originalPartition, originalOffset, event, State.FAILED)
  }

  override fun processEvent(record: ConsumerRecord<String, String>) {
    // main business logic should be here
    LOG.info("Processing Event")
    if (record.key() == "{\"name\":\"emmanuel_okuchukwu\"}") throw RuntimeException("Simulating Error for key: ${record.key()}")
    if (record.key() == "{\"name\":\"john_marston\"}") throw RuntimeException("Simulating Error for key: ${record.key()}")
    if (record.key() == "{\"name\":\"arthur_morgan\"}") throw RuntimeException("Simulating Error for key: ${record.key()}")
    if (record.key() == "{\"name\":\"anoj_kunes\"}") throw RuntimeException("Simulating Error for key: ${record.key()}")
    if (record.key() == "{\"name\":\"tester\"}") throw RuntimeException("Simulating Error for key: ${record.key()}")
    if (record.key() == "{\"name\":\"flopper\"}") throw RuntimeException("Simulating Error for key: ${record.key()}")
  }
}