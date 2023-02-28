package com.ak.ms.kafkafailure.config

import com.ak.ms.kafkafailure.exception.DeadLetterException
import com.ak.ms.kafkafailure.exception.RetryException
import com.ak.ms.kafkafailure.mongo.repository.RetryStateMongoRepository
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultBackOffHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS
import org.springframework.util.backoff.FixedBackOff
import org.apache.commons.lang3.exception.ExceptionUtils
import org.springframework.kafka.support.KafkaHeaders

@Configuration
class KafkaConsumerConfig(@Value(value = "\${spring.kafka.bootstrap-servers}") val bootstrapAddress: String) {
  private val logger = KotlinLogging.logger {}

  @Bean
  fun consumerFactory(): ConsumerFactory<String, Any> {
    val configProps: MutableMap<String, Any> = HashMap()
    configProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapAddress
    configProps[KEY_DESERIALIZER_CLASS] = StringDeserializer::class.java
    configProps[VALUE_DESERIALIZER_CLASS] = StringDeserializer::class.java
    configProps[KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    configProps[VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    return DefaultKafkaConsumerFactory(configProps)
  }

  @Bean
  fun kafkaListenerContainerFactory(
    kafkaTemplate: KafkaTemplate<String, Any>,
    kafkaOperations: KafkaOperations<*, *>,
    customKafkaTopicProperties: CustomKafkaTopicProperties
  ): ConcurrentKafkaListenerContainerFactory<String, Any> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
    factory.consumerFactory = consumerFactory()
    factory.setCommonErrorHandler(
      DefaultErrorHandler(
        DeadLetterPublishingRecoverer(
          kafkaOperations
        ) { cr: ConsumerRecord<*, *>, e: Exception? ->
          logger.info("Received Error From Topic:${cr.topic()} and Partition: ${cr.partition()}", e)
          cr.headers().add("RETRYING_REFERENCE_ID", "${cr.partition()}:${cr.offset()}".toByteArray())
          cr.headers().add("RETRYING_ORDER", "UNSORTED".toByteArray())
          cr.headers().add(KafkaHeaders.ORIGINAL_PARTITION, "${cr.partition()}".toByteArray())
          cr.headers().add(KafkaHeaders.ORIGINAL_OFFSET, "${cr.offset()}".toByteArray())
          TopicPartition(
            "${cr.topic()}_${customKafkaTopicProperties.consumer.groupId}_RETRY",
            cr.partition()
          )
        },
        FixedBackOff(0, 0)
      )
    )
    factory.containerProperties.isObservationEnabled = true
    return factory
  }

  @Bean
  fun kafkaRetryListenerContainerFactory(
    kafkaTemplate: KafkaTemplate<String, Any>,
    kafkaOperations: KafkaOperations<*, *>,
  ): ConcurrentKafkaListenerContainerFactory<String, Any> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
    factory.consumerFactory = consumerFactory()
    factory.setCommonErrorHandler(
      DefaultErrorHandler(
        DeadLetterPublishingRecoverer(
          kafkaOperations
        ) { cr: ConsumerRecord<*, *>, e: Exception? ->
          TopicPartition(
            cr.topic().replace("_RETRY", "_ERROR"),
            cr.partition()
          )
        },
        FixedBackOff(3000, 3)
      )
    )
    factory.containerProperties.isObservationEnabled = true
    return factory;
  }

  @Bean
  fun kafkaDeadLetterContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
    factory.consumerFactory = consumerFactory()
    factory.containerProperties.isObservationEnabled = true
    return factory;
  }

  @Bean
  fun maintainingOrderKafkaListenerContainerFactory(
    kafkaTemplate: KafkaTemplate<String, Any>,
    kafkaOperations: KafkaOperations<*, *>,
    customKafkaTopicProperties: CustomKafkaTopicProperties,
    retryStateMongoRepository: RetryStateMongoRepository
  ): ConcurrentKafkaListenerContainerFactory<String, Any> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
    factory.consumerFactory = consumerFactory()

    val deadLetterPublishingRecoverer = DeadLetterPublishingRecoverer(
      kafkaOperations,
    ) { cr: ConsumerRecord<*, *>, e: Exception? ->
      logger.info("Received Error From Topic:${cr.topic()} and Partition: ${cr.partition()}", e)
      cr.headers().add("RETRYING_REFERENCE_ID", "${cr.partition()}:${cr.offset()}".toByteArray())
      cr.headers().add("RETRYING_ORDER", "SORTED".toByteArray())
      cr.headers().add(KafkaHeaders.ORIGINAL_PARTITION, "${cr.partition()}".toByteArray())
      cr.headers().add(KafkaHeaders.ORIGINAL_OFFSET, "${cr.offset()}".toByteArray())

      if (ExceptionUtils.indexOfType(e, DeadLetterException::class.java) != -1) {
        TopicPartition(
          "${cr.topic()}_${customKafkaTopicProperties.consumer.groupId}_ERROR",
          cr.partition()
        )
      } else {
        TopicPartition(
          "${cr.topic()}_${customKafkaTopicProperties.consumer.groupId}_RETRY",
          cr.partition()
        )
      }
    }

    factory.setCommonErrorHandler(
      DefaultErrorHandler(
        deadLetterPublishingRecoverer,
        FixedBackOff(0, 0)
      )
    )

    factory.containerProperties.isObservationEnabled = true
    return factory
  }


  @Bean
  fun maintainingOrderKafkaRetryListenerContainerFactory(
    kafkaTemplate: KafkaTemplate<String, Any>,
    kafkaOperations: KafkaOperations<*, *>,
    retryStateMongoRepository: RetryStateMongoRepository
  ): ConcurrentKafkaListenerContainerFactory<String, Any> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
    factory.consumerFactory = consumerFactory()

    val deadLetterPublishingRecoverer = DeadLetterPublishingRecoverer(
      kafkaOperations
    ) { cr: ConsumerRecord<*, *>, e: Exception? ->
      TopicPartition(
        cr.topic().replace("_RETRY", "_ERROR"),
        cr.partition()
      )
    }

    val errorHandler = DefaultErrorHandler(
      deadLetterPublishingRecoverer,
      FixedBackOff(3000, 3),
    )

    errorHandler.addNotRetryableExceptions(
      RetryException::class.java,
      DeadLetterException::class.java
    )

    factory.setCommonErrorHandler(errorHandler)
    factory.containerProperties.isObservationEnabled = true

    return factory;
  }

  @Bean
  fun maintainingOrderKafkaDeadLetterContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
    factory.consumerFactory = consumerFactory()
    factory.containerProperties.isObservationEnabled = true
    return factory;
  }
}