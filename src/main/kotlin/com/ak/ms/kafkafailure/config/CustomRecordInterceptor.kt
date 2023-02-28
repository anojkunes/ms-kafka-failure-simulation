package com.ak.ms.kafkafailure.config

import java.util.concurrent.atomic.AtomicInteger
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.listener.RecordInterceptor

class CustomRecordInterceptor: RecordInterceptor<String, Any>{
  private val logger = KotlinLogging.logger {}

  override fun success(record: ConsumerRecord<String, Any>, consumer: Consumer<String, Any>) {
    throw RuntimeException("Throwing message for your ${record.offset()}_${record.partition()}_${record.key()}")
  }

  override fun intercept(
    record: ConsumerRecord<String, Any>,
    consumer: Consumer<String, Any>
  ): ConsumerRecord<String, Any>? {
    logger.info("Intercepted message for your ${record.offset()}_${record.partition()}_${record.key()}")
    return record
  }
}