package com.ak.ms.kafkafailure.kafka.consumer

import com.ak.ms.kafkafailure.exception.DeadLetterException
import com.ak.ms.kafkafailure.exception.RetryException
import com.ak.ms.kafkafailure.mongo.entity.RetryState
import com.ak.ms.kafkafailure.mongo.entity.RetryStateId
import com.ak.ms.kafkafailure.mongo.entity.State
import com.ak.ms.kafkafailure.mongo.repository.RetryStateMongoRepository
import java.time.LocalDateTime
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.data.domain.Sort
import org.springframework.stereotype.Component

@Component
abstract class AbstractMaintainingOrderKafkaHandler(
  val retryStateMongoRepository: RetryStateMongoRepository
) {

  private val LOG = KotlinLogging.logger {}

  fun handleEvent(
    originalPartition: Int,
    originalOffset: Long,
    event: ConsumerRecord<String, String>,
    state: State
  ) {
    try {
      isPreviousEventsRetrying(
        partition = originalPartition,
        offset = originalOffset,
        primaryIdentifier = event.key(),
        record = event
      )
      // Main Logic
      processEvent(event)

      // Committed
      removeEventRetrying(originalPartition, originalOffset)
    } catch (ex: DeadLetterException) {
      LOG.info("throwing DeadLetterException")
      throw ex
    } catch (ex: RetryException) {
      LOG.info("throwing RetryException")
      throw ex
    } catch (ex: Exception) {
      saveRetryState(originalPartition, originalOffset, event, state)
      throw ex
    }
  }

  abstract fun processEvent(record: ConsumerRecord<String, String>)

  fun isPreviousEventsRetrying(partition: Int, offset: Long, record: ConsumerRecord<String, String>, primaryIdentifier: String) {
    val previousRetryState = retryStateMongoRepository.findFirstByPrimaryIdentifierAndId_PartitionAndId_OffsetLessThan(
      partition = partition,
      offset = offset,
      primaryIdentifier = primaryIdentifier,
      sort = Sort.by(Sort.Order(Sort.Direction.ASC, "_id.partition"), Sort.Order(Sort.Direction.DESC, "_id.offset"))
    )

    LOG.info("previous state: $previousRetryState")

    previousRetryState?.run {
      LOG.info("previousState RETRYING: ${state == State.RETRYING.name}")
      LOG.info("previousState FAILED: ${state == State.FAILED.name}")

      if (state == State.RETRYING.name) {
        saveRetryState(partition, offset, record, State.RETRYING)
        throw RetryException()
      } else if (state == State.FAILED.name) {
        saveRetryState(partition, offset, record, State.FAILED)
        throw DeadLetterException()
      }
    }
  }

  fun removeEventRetrying(partition: Int, offset: Long) {
    retryStateMongoRepository.deleteById(RetryStateId(partition, offset))
  }

  fun saveRetryState(partition: Int, offset: Long, record: ConsumerRecord<String, String>, state: State): RetryState {
    val retryStateFound = retryStateMongoRepository.findById(RetryStateId(partition, offset)).map { it.copy(state = state.name, topic = record.topic()) }
    if (retryStateFound.isPresent) {
      LOG.debug("Retry State found for Original Partition: $partition and Original Offset: $offset")
      return retryStateMongoRepository.save(retryStateFound.get())
    }

    LOG.debug("Retry State NOT found for Original Partition: $partition and Original Offset: $offset, creating a new Retry State")
    val retryState = RetryState(
      id = RetryStateId(partition, offset),
      topic = record.topic(),
      primaryIdentifier = record.key(),
      state = state.toString(),
      timestamp = record.timestamp(),
      createdAt = LocalDateTime.now()
    )

    return retryStateMongoRepository.save(retryState)
  }
}