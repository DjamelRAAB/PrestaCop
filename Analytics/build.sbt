name := "Spark Project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.5"
libraryDependencies += "com.google.code.gson" % "gson" % "2.7"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1",
  "org.apache.spark" %% "spark-streaming" % "2.1.1",
  "org.apache.spark" %% "spark-hive" % "2.1.1",
  "mysql" % "mysql-connector-java" % "5.1.6"
)
