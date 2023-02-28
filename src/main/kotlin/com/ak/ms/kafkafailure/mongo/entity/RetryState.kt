package com.ak.ms.kafkafailure.mongo.entity

import java.time.LocalDateTime
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

@Document("retry-state")
data class RetryState (
  @Id
  val id: RetryStateId,
  val topic: String,
  val primaryIdentifier: String,
  val state: String,
  val timestamp: Long,
  val createdAt: LocalDateTime
)
