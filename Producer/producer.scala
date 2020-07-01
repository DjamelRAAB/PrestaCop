import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source


object ScalaProducerExample extends App {

  val producerProperties = new Properties()
  
  producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
  producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  
  val producer = new KafkaProducer[String, String](producerProperties)

  val lines = Source.fromFile("data.txt").getLines.toList.map(x=>{  // we read the txt file which contains our data about drones
  //drone_id , stat_message , drone_latitude , drone_longitude , time , battery , temperature , violation_code , image_id
      val record = new ProducerRecord[String, String]("iot" , x) //we produce in the topic iot all kind of data
      producer.send(record)
      Thread.sleep(2000)
    }
  )
  
  producer.close()

}
