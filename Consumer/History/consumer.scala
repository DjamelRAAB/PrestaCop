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
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiverHistory")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(3))
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
    Set("history") // we subscribe our consummer to the history iot which contains all the data record by drones
    )

  kafkaStreams.print()
  kafkaStreams.foreachRDD( rdd => {
    if(!rdd.isEmpty()){ 
      rdd.toDF.write.format("csv").mode(SaveMode.Append).save("../../HDFS/HISTORY/") 
    }
  })
  
  ssc.start()
  ssc.awaitTermination()
}