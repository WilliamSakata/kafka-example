import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer
import java.io.File
import java.util.Properties
import java.util.Random
import java.util.Timer
import java.util.TimerTask
import kotlin.random.Random.Default.nextLong
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

fun properties(): Properties {
    val properties = Properties()

    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer::class.qualifiedName)

    return properties
}

fun main() {

    val producer = KafkaProducer<String, GenericRecord>(properties())

    val schema = Schema.Parser().parse(File("src/main/resources/SCHEMA.avsc"))

    val timer = Timer()

    timer.scheduleAtFixedRate(
        object : TimerTask() {
            override fun run() {

                val randomCity = Cities.values()[Random().nextInt(Cities.values().size)].name
                val randomTemperature = nextLong(-20, 50)
                val randomHumidity = nextLong(-20, 50)

                val avroClimate = GenericRecordBuilder(schema).apply {
                    set("temperature", randomTemperature)
                    set("humidity", randomHumidity)
                }.build()

                val response = producer.send(ProducerRecord("test.topic", randomCity, avroClimate))

                val metadata = response.get()

                println("Topic:${metadata.topic()}\nPartition: ${metadata.partition()}\nOffset:${metadata.offset()}\n")
            }
        }, 0, 10000
    )
}
