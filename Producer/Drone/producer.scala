import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
import java.io.File
import javax.imageio.ImageIO
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import java.nio.file.{Files, Paths}

object ScalaProducerDrone extends App {

  case class Message(
    drone_id : Double,
    stat_message : Double,
    drone_latitude : Double,
    drone_longitude : Double,
    time : Double,
    battery : Double,
    temperature : Double,
    violation_code : Option[Double] = None,
    image: Option[Int] = None
  )

  def fakeMessage () : Message = {
    Message(drone_id = 1, 
            stat_message = 1,
            drone_latitude = 1, 
            drone_longitude = 1, 
            time = 1, 
            battery = 1, 
            temperature = 1, 
            violation_code = Some(1), 
            image = Some(1))
  }

  def serializerMessage (message : Message) : String = {
    // Function which serialize case class Message to String
    implicit val MessageWrites = Json.writes[Message]
    val messageJson: JsValue = Json.toJson(message)
    val messageString = Json.stringify(messageJson)
    messageString
  }

  // Create KafkaProducer[String, String] connected to 127.0.0.1:9092 server
  val producerProperties = new Properties()
  producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
  producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](producerProperties)

  val producerPropertiesImages = new Properties()
  producerPropertiesImages.put("bootstrap.servers", "127.0.0.1:9092")
  producerPropertiesImages.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerPropertiesImages.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  val producerImages = new KafkaProducer[String, Array[Byte]](producerPropertiesImages)

  // Here we read the txt file which contains our data simulation about drones
  val lines = Source.fromFile("data.txt").getLines.toList.map(line=>{  
    producer.send(new ProducerRecord[String, String]("iot" , line)) 
    Thread.sleep(2000)
  })

  // In real case we just need to change calling fakeMessage by generate real message function 
  
  while(true){
    val message = fakeMessage()
    producer.send(new ProducerRecord[String, String]("iot" , serializerMessage(message))) 
    message.image match{
      case Some(id) => {
        val byteArray = Files.readAllBytes(Paths.get("./input_images/"+id+".jpg"))
        producerImages.send(new ProducerRecord[String, Array[Byte]]("images", id.toString,byteArray))
      }
      case None =>
    }
    Thread.sleep(2000)
  }
  producer.close()
}


/*
message.image match{
      case Some(id) => bucket.put(id +".jpg", new File("./input_images/"+ id +".jpg"))
      case None =>
    }
*/