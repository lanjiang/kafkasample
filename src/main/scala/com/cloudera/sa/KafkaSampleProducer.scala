package com.cloudera.sa

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}


/**
  * Created by ljiang on 1/20/17.
  */
object KafkaSampleProducer extends App{
  println("Hello World")
  val kafkaProps: Properties = new Properties()
  kafkaProps.put("bootstrap.servers", "ljiang-kafka-2.vpc.cloudera.com:9092")
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer:KafkaProducer[String,String] = new KafkaProducer(kafkaProps)
  var i = 0
  for (i <- 1 until 10)
  {
    val record: ProducerRecord[String, String] = new ProducerRecord("t1", "key" + i, "value" + i)
    try {
      println("before sending")
      //producer.send(record)
      //producer.send(record).get()
      producer.send(record, new MyCallBack)

      println("done sending")
    }
    catch {
      case e: Exception => println(e)
      case _: Throwable => println("something is very wrong")
    }

  }
  // Below sleep is not required if using  producer.send(record).get() because the get makes it a block call.
  Thread.sleep(500)

  class MyCallBack extends Callback {
    def onCompletion(recordMetadata: RecordMetadata, exception: Exception) = {
      println("in callback")
       if (exception != null) {
         exception.printStackTrace()
       }
    }

  }
}



