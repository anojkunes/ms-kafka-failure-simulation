package com.ak.ms.kafkafailure.mongo.repository

import com.ak.ms.kafkafailure.mongo.entity.RetryState
import com.ak.ms.kafkafailure.mongo.entity.RetryStateId
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface RetryStateMongoRepository : MongoRepository<RetryState, RetryStateId> {
  fun findFirstByPrimaryIdentifierAndId_PartitionAndId_OffsetLessThan(primaryIdentifier: String, partition: Int, offset: Long, sort: Sort): RetryState?
}