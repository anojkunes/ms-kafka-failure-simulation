package com.ak.ms.kafkafailure.api.request

import io.swagger.v3.oas.annotations.media.Schema

@Schema(description = "Kafka Message Request")
data class KafkaMessageRequest(
  @Schema(description = "Kafka Payload", example = """
    {
        "eventType": "CREATED",
        "provider_id": 271,
        "clients": [
            {
                "ytree_client_id": "56b5f4e1-099a-4f73-95af-96bda166aafb",
                "client_id": "8b014867-4504-4c21-b286-cb91a06b61ec",
                "accounts": [
                    {
                        "number": "70082261",
                        "type": "CURRENT_ACCOUNT",
                        "statement_date": "2022-09-13",
                        "currency": "GBP",
                        "balance": 233.63,
                        "transactions": [
                            {
                                "type": "DEPOSIT",
                                "date": "2022-08-31",
                                "amount": 0.96,
                                "currency": "GBP",
                                "description": "Starling Bank",
                                "id": "aba89e09-f941-4667-a89b-4b9a0a85b4b3"
                            }
                        ],
                        "source": "YAPILY",
                        "externalSource": "OPEN_BANKING",
                        "externalId": "4d6b28ee-906f-449d-9e31-7a22a33d2389"
                    }
                ]
            }
        ],
        "source": "YAPILY"
    }
  """
  )
  val message: Any,
  @Schema(description = "Kafka Key", example = "{\"name\": \"tester\"}")
  val key: String
)