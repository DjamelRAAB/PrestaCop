import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source



object ScalaProducerHistory extends App {
  // Create KafkaProducer[String, String] connected to 127.0.0.1:9092 server
  val producerProperties = new Properties()
  producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
  producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](producerProperties)

  val bufferedSource = Source.fromFile("data.csv")
  bufferedSource.getLines.drop(1).toList.foreach{ line =>  
    producer.send(new ProducerRecord[String, String]("history" , line)) 
  }

  bufferedSource.close
  producer.close()
}


