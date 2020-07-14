import java.util
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext



object Analytics extends App {
  /*
  Command to do not display the logs
  */
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf()
                      .setAppName("Analytics")
                      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.


  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
)

  val textFile = sqlContext.read.parquet ("../HDFS")

  val id = 0 // postion of id in data stored
  val latitude = 1 // position of latitude in the text file
  val battery = 4 // position of battery in the text file
  val temperature = 5 // position of temperature in the text file
  val violation_code = 6 // position of violation_code in the text file

  /*
  common Violation 
  percentage violation
  */
  val commonViolation = textFile.filter(x=>{
    val split = x.split(";")
    if(split(violation_code) != "None" )
  })
  .map(x=>{
    val split = x.split(";")
    (split(violation_code).toDouble,1)
  }).reduceByKey((accum, n) => (accum + n))
  val commonViolation_code = commonViolation.reduce((x, y) => if(x._2 > y._2) x else y)


  /*
  In proportion to the whole number of violation is there more violation
  in the north hemisphere or the south hemisphere
  */
  val violationHemisphere = textFile.filter(x=>{
    val split = x.split(";")
    if(split(violation_code) != "None" )
  })
  .map(x=>{
    val split = x.split(";")
    if(split(latitude).toDouble> 0.0) {
      ("Hemisphere_nord",1)
    }
    else {
      ("Hemisphere_nord",0)
    }
    if(split(latitude).toDouble < 0.0 ){
      ("Hemisphere_sud",1)
    }
    else{
      ("Hemisphere_sud",0)
    }
  }).reduceByKey((accum, n) => (accum + n))
  val maxFailHemis = violationHemisphere.reduce((x, y) => if(x._2 > y._2) x else y)

  
  /*
  Is there more failing devices when the weather
  is hot or when the weather is called
  */
  val statsFailingTemperature = textFile.map(x=>{
    val split = x.split(";")
    if(split(temperature).toDouble < 10.0 && split(battery).toFloat == 0.0 ) {
      ("Cold",1)
    }
    else if(split(temperature).toDouble > 20.0 && split(battery).toFloat == 0.0 ){
      ("Hot",1)
    }
    else{
      ("Cold",0)
    }
  }).reduceByKey((accum, n) => (accum + n))
  val maxFailTemp = statsFailingTemperature.reduce((x, y) => if(x._2 > y._2) x else y)

  /*
  Among the failing devices which percentage fails beceause of low battery?
  */
  val statsFailingBattery = textFile.map(x => {
    val split = x.split(";")
    if(split(battery).toFloat == 0.0 ) {
      ("low_battery",1)
    }
    else{
      ("low_battery",0)
    }
  }).reduceByKey((accum, n) => (accum + n))
  val pourcLowBat = statsFailingBattery.map(x => x._2.toFloat/textFile.count * 100)

  /*
  number of failling per drone
  */
  val droneFailling = textFile.map(x => {
    val split = x.split(";")
    if(split(battery).toFloat == 0){
      val temp = split(id)
      (s"id:$temp", 1)
    }
    else {
      val temp = split(id)
      (s"id:$temp", 0)
    }
  }).reduceByKey((accum, n) => (accum + n))


  //Displaying the statistics that we have calculated
  println("---------------------Spark Analytics with RDDs-------------------")

  println("----------------------------Question 1----------------------")
  println("common Violation ")
  println("percentage violation ")
  println(s"Answer : $commonViolation_code")
  println("------------------------------------------------------------")

  println("----------------------------Question 2----------------------")
  println("In proportion to the whole number of violation is there more violation")
  println("in the north hemisphere or the south hemisphere ")
  statsFailingHemisphere.foreach(println)
  println(s"Answer : $maxFailHemis")
  println("------------------------------------------------------------")

  println("----------------------------Question 3----------------------")
  println("Is there more failing devices when the weather")
  println("is hot or when the weather is cold : ")
  statsFailingTemperature.foreach(println)
  println(s"Answer : $maxFailTemp")
  println("------------------------------------------------------------")

  println("----------------------------Question 4----------------------")
  println("Among the failing devices which percentage fails beceause of low battery :")
  statsFailingBattery.foreach(println)
  print("Answer : ")
  pourcLowBat.foreach(print)
  println(" %")
  println("------------------------------------------------------------")

  println("----------------------------Question 5----------------------")
  println("Number of failling per drone : ")
  droneFailling.foreach(println)
  println("------------------------------------------------------------")

  //Stop Spark SparkContext
  sc.stop()
}
