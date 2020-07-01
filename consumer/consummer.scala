import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import java.io._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object ScalaConsumerExample extends App {

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

  val kafkaStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    Map("metadata.broker.list" -> "127.0.0.1:9092","bootstrap.servers"-> "127.0.0.1:9092"),
    Set("iot") // we subscribe our consummer to the topic iot which contains all the data record by drones
    )

  kafkaStreams.print()
  kafkaStreams.foreachRDD( rdd => {
    if(rdd.count()>0)
    {
      rdd.toDF.write.format("parquet").mode(SaveMode.Append).save("../HDFS") //we stored like in hdfs all the data catch by the consummer
      rdd.map(x => {
        val split = x._2.split(',') // we split the value of the tuple to get each element
        if(split(1).toInt == 2) { // we check if the third element get previously splitting whic correspond to stat message is equals to 2
          val record = new ProducerRecord[String, String]("alert" , split.mkString(",")) // if stat message is equal to 2, we sent the information of the drones concerns to the topic alert
          producer.send(record)
        }
      }).foreach(println)
    }
  })

  ssc.start()
  ssc.awaitTermination()

}
