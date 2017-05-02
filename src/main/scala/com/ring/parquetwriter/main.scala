package com.ring.parquetwriter

import java.util.UUID
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.avro.Schema
import org.apache.commons.logging.LogFactory
import scala.io.Source

class ParquetWriter(){

}

object ParquetWriter {

  private val log = LogFactory.getLog(classOf[ParquetWriter])

  private def checkUsage(args: Array[String]): Unit = {
    if(args.length != 4) {
      println(s"Usage ${classOf[ParquetWriter]} <application name> <stream name> <schema file> <bucket name>")
      System.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    checkUsage(args)

    val applicationName = args(0)
    val streamName = args(1)
    val schemaFile = args(2)

    val fileContents = Source.fromFile(schemaFile).getLines.mkString
    val schema = new Schema.Parser().parse(fileContents)
    // announce schema

    val bucketName = args(3)

    val writer = new BufferedWriter(schema, bucketName)
    val recordProcessorFactory = new RecordProcessorFactory(writer)

    val instanceProfileCredentialsProvider = new InstanceProfileCredentialsProvider()
    val workerId = String.valueOf(UUID.randomUUID)
    val kclConfig = new KinesisClientLibConfiguration(applicationName, streamName, instanceProfileCredentialsProvider, workerId)
      .withCommonClientConfig(new ClientConfiguration())

    val worker = new Worker(recordProcessorFactory, kclConfig)

    var exitCode = 0
    try
      worker.run()
    catch {
      case t: Throwable =>
        log.error("Caught throwable while processing data.", t)
        exitCode = 1
    }
    System.exit(exitCode)
  }

}
