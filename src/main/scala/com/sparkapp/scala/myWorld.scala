package com.sparkapp.scala


import java.util.Properties
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}

import org.apache.spark.SparkContext
object myWorld {

  def main(args: Array[String]) {

    val props = new Properties()
    props.put("bootstrap.servers", "wn01.itversity.com:6667 ")
    //props.put("acks", "1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val topic = "kafkaNik"


    for (i <- 1 to 50) {
      import org.apache.kafka.clients.producer.RecordMetadata

      val record = new ProducerRecord(topic, "key" + i, "value" + i)
      val metadata = producer.send(record).get
      producer.send(record)
      println("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key, record.value, metadata.partition, metadata.offset)    }

    producer.close()
  }
}
