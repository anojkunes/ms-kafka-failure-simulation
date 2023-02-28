package com.ak.ms.kafkafailure.mongo.entity

data class RetryStateId(
  val partition: Int,
  val offset: Long
)