import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import java.io._
import jp.co.bizreach.s3scala.S3
import awscala.s3._
import awscala.Region
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

object ScalaConsumerImagesS3 extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiverImages")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))
  val kafkaStreams = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
    ssc,
    Map("metadata.broker.list" -> "127.0.0.1:9092","bootstrap.servers"-> "127.0.0.1:9092"),
    Set("images") // we subscribe our consummer to the topic images which contains all the data record by drones
    )

  val config = new Configuration()
  config.set("fs.s3a.access.key", "AKIAZYGYRIHTTXXSDCWG")
  config.set("fs.s3a.secret.key", "******")
  config.set("fs.s3a.endpoint", "s3.amazonaws.com")

  kafkaStreams.foreachRDD(rdd => {
    if(!rdd.isEmpty()){ 
      rdd.map(x =>{
        val dest = new Path("s3a://prestacop/"+x._1+".png")
        val fs = dest.getFileSystem(config)
        val out = fs.create(dest, true)
        out.write(x._2)
        out.close()
        x
      })
    }
  })
  
  ssc.start()
  ssc.awaitTermination()

}

/*
  implicit val region = Region.Ireland
  implicit val s3 = S3(accessKeyId = "AKIAZYGYRIHTTXXSDCWG", secretAccessKey = "******")
  val bucket: Bucket = s3.createBucket("unique-name-dja2") 
  bucket.put(x._1+".jpg", new File("tmp.jpg"))
*/