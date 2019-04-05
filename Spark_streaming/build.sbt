name := "SparkStreaming"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.1"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)