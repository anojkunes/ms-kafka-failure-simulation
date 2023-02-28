package com.ak.ms.kafkafailure.config

import com.ak.ms.kafkafailure.mongo.repository.RetryStateMongoRepository
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.config.EnableMongoAuditing
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories

@Configuration
@EnableMongoRepositories(basePackageClasses = [RetryStateMongoRepository::class])
class MongoConfiguration
