package com.ak.ms.kafkafailure.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "spring.kafka")
data class CustomKafkaTopicProperties(
  val topics: List<KafkaTopicProperty>,
  val consumer: KafkaConsumerProperty
) {
  data class KafkaTopicProperty(
    var name: String = "",
    var replicaFactor: Int = 0,
    var partition: Int = 0
  )

  data class KafkaConsumerProperty(
    var groupId: String = ""
  )
}

