import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import java.io._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

object ScalaConsumerMessageProducerAlert extends App {

  case class Message(
    drone_id : Double,
    stat_message : Double,
    drone_latitude : Double,
    drone_longitude : Double,
    time : Double,
    battery : Double,
    temperature : Double,
    violation_code : Option[Double] = None,
    image: Option[Double] = None
  )
  
  implicit val MessageReads = Json.reads[Message]
  implicit val MessageWrites = Json.writes[Message]

  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val producerProperties = new Properties()
  producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
  producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](producerProperties)

  val kafkaStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    Map("metadata.broker.list" -> "127.0.0.1:9092","bootstrap.servers"-> "127.0.0.1:9092"),
    Set("iot") // we subscribe our consummer to the topic iot which contains all the data record by drones
    )

  kafkaStreams.print()
  kafkaStreams.foreachRDD( rdd => {
    if(rdd.count()>0)
    {
      rdd.map(x => {
        val messageJson: JsValue = Json.parse(x._2)
        messageJson.validate[Message] match {
          case s: JsSuccess[Message] => {            
            val message: Message = s.get //we get in the topic iot good data
            message.violation_code match {
              case Some(4) =>{ //we check if violation id is equals to 4 if it's then it's an alert
                val messageJson: JsValue = Json.toJson(message)
                val messageToSend = Json.stringify(messageJson)
                producer.send(new ProducerRecord[String, String]("alert" , messageToSend)) 
              }
              case _ => {
                println("-----This message is not an alert i don't take it------")
                }
            }
          }
          case e: JsError => {// error handling flow
            println("____! Corrupted Message was trashed !____") 
          }  
        }
      }).foreach(println)
    }
  })
  
  ssc.start()
  ssc.awaitTermination()

}
