package com.ak.ms.kafkafailure.api

import com.ak.ms.kafkafailure.api.request.KafkaMessageRequest
import com.ak.ms.kafkafailure.kafka.producer.KafkaProducer
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import mu.KotlinLogging
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*

@RestController
@Tag(name = "Sending Kafka Messages", description = "Endpoints to send Messages to Kafka")
@RequestMapping(value = ["/kafka-failure/v1/internal"], produces = [MediaType.APPLICATION_JSON_VALUE])
class KafkaFailureController(
  val kafkaProducer: KafkaProducer
) {

  private val logger = KotlinLogging.logger {}

  @Operation(
    summary = "Send Message",
    description = "Sending Message to Kafka Topic called some-topic",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "200",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = Unit::class))
        ]
      )
    ]
  )
  @PostMapping
  fun sendMessage(
    @Schema(
      description = "Kafka Message to Send",
      example = """
        {
          "name": "Sam Stranding"
        }
      """,
      required=true
    )
    @RequestBody
    message: Any
  ) {
    val key = UUID.randomUUID().toString()
    logger.info("Sending message to some-topic: $message for key: $key")
    kafkaProducer.sendMessage("some-topic", key, message)
  }

  @Operation(
    summary = "Send Ordered Message",
    description = "Sending Ordered Message to Kafka Topic called maintaining-order-event",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "200",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = Unit::class))
        ]
      )
    ]
  )
  @PostMapping("/ordered")
  fun sendOrderedMessage(@RequestBody message: KafkaMessageRequest) {
    logger.info("Sending message to maintaining-order-event: $message for key: ${message.key}")
    kafkaProducer.sendMaintainingOrderMessage("maintaining-order-event", message.key, message.message)
  }

}