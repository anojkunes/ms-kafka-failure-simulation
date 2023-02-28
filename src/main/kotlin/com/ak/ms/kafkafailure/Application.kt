package com.ak.ms.kafkafailure

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan(basePackages = ["com.ak.ms.kafkafailure.*"])
class Application

fun main(args: Array<String>) {
	runApplication<Application>(*args)
}
