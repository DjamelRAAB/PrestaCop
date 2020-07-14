name := "TEST-CONSUMMER"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.0"



libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion


libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.11"
