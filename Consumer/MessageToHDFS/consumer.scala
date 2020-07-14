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

object ScalaConsumerExample extends App {

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

  object MyFunctions {
    // Function which check if the recived json is valide Message and transforme it to String
    def MessageToString (messageString : String) : String = {
      val messageJson: JsValue = Json.parse(messageString)
      messageJson.validate[Message] match {
        case s : JsSuccess[Message] => {
          val message: Message = s.get 
            (message.violation_code,message.image) match {
              case (Some(a),Some(b)) => "%f;%f;%f;%f;%.1f;%f;%f;%f".format(message.drone_id,message.drone_latitude,message.drone_longitude,message.time,message.battery,message.temperature,a,b)
              case (Some(a),None)    => "%f;%f;%f;%f;%.1f;%f;%f;None".format(message.drone_id,message.drone_latitude,message.drone_longitude,message.time,message.battery,message.temperature,a)
              case (None,Some(b))    => "%f;%f;%f;%f;%.1f;%f;None;%f".format(message.drone_id,message.drone_latitude,message.drone_longitude,message.time,message.battery,message.temperature,b)
              case (None,None)       => "%f;%f;%f;%f;%.1f;%f;None;None".format(message.drone_id,message.drone_latitude,message.drone_longitude,message.time,message.battery,message.temperature)
            }
          }
        case e: JsError => {// error handling flow
          println("____! Corrupted Message was trashed !____") 
          ""
        }  
      }
    }
  }
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(3))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  

  val producerProperties = new Properties()
  producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
  producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](producerProperties)

  def serializer_message (message : Message) : String = {
    // Function which serialize case class Message to String
    implicit val MessageWrites = Json.writes[Message]
    val messageJson: JsValue = Json.toJson(message)
    val messageString = Json.stringify(messageJson)
    messageString
  }
  val kafkaStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    Map("metadata.broker.list" -> "127.0.0.1:9092","bootstrap.servers"-> "127.0.0.1:9092"),
    Set("iot") // we subscribe our consummer to the topic iot which contains all the data record by drones
    )
  kafkaStreams.print()
  kafkaStreams.foreachRDD( rdd => {
    if(!rdd.isEmpty()){ 
      rdd.filter(x => {
        val messageJson: JsValue = Json.parse(x._2)
        messageJson.validate[Message] match {
          case s : JsSuccess[Message] => {
            true
            }
          case e: JsError => {// error handling flow
            println("____! Corrupted Message was trashed !____") 
            false
          }  
        }
      })//we save like in hdfs all the good data catched by the consummer
      .toDF.write.format("parquet").mode(SaveMode.Append).save("../../HDFS") 
    }
  })
  
  ssc.start()
  ssc.awaitTermination()

}
/*
      val filtredRDD = rdd.map(x=>MessageToString(x._2))
      filtredRDD.map(x => {
        val rec = new ProducerRecord[String, String]("test" , x ) 
        producer.send(rec)
      }).foreach(println)
      //.toDF.write.format("parquet").mode(SaveMode.Append).save("../HDFS") 
      */