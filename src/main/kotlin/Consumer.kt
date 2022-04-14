import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import java.time.Duration
import java.util.Properties
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG

fun props(): Properties {
    val properties = Properties()

    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer::class.qualifiedName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_test")
    properties.setProperty(APPLICATION_ID_CONFIG, "app_test")

    return properties
}

fun main() {

    val kafkaConsumer = KafkaConsumer<String, GenericRecord>(props())

    kafkaConsumer.subscribe(listOf("test.topic"))

    while (true) {
        val registers = kafkaConsumer.poll(Duration.ofMillis(1000))

        registers.forEach { register ->
            println(
                messageFormatter(
                    register.key(), register.value().toString(), register.offset(), register.partition()
                )
            )
        }
    }
}

fun messageFormatter(key: String, value: String, offset: Long, partition: Int): String {
    return """
        
        KEY: $key
        
        Value: $value
        
        Offset: $offset
        
        Partition: $partition
        
    """.trimIndent()
}