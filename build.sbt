import Dependencies._

scalacOptions := Seq("-unchecked", "-deprecation")
//mainClass in assembly := Some("com.ring.testKinesis.consumer.TicketReader")
assemblyJarName in assembly := "sample-kinesis-assembly-0.1.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.ring",
      scalaVersion := "2.12.1",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "sample-kinesis",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "com.amazonaws" % "amazon-kinesis-client" % "1.6.3",
      "org.apache.avro" % "avro" % "1.8.1",
      "org.apache.parquet" % "parquet-avro" % "1.8.1",
      "org.apache.hadoop" % "hadoop-common" % "2.8.0"
    )
  )
