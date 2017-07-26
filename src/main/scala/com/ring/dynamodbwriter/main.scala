package com.ring.dynamodbwriter

import java.util.UUID

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.{AWSCredentialsProviderChain, EnvironmentVariableCredentialsProvider, InstanceProfileCredentialsProvider}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import org.apache.avro.Schema
import org.apache.commons.logging.LogFactory

class DynamoDBWriter(){

}

object DynamoDBWriter {

  private val log = LogFactory.getLog(classOf[DynamoDBWriter])

  private def checkUsage(args: Array[String]): Unit = {
    if(args.length != 4) {
      println(s"Usage ${classOf[DynamoDBWriter]} <application name> <stream name> <table name> <batch size>")
      System.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    checkUsage(args)

    val applicationName = args(0)
    val streamName = args(1)
    val tableName = args(2)
    val batchSize = args(3)

   // val instanceProfileCredentialsProvider = new InstanceProfileCredentialsProvider()

    ///val envCredentialsProvider = new EnvironmentVariableCredentialsProvider()

    val credentialsProvider = new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(), new InstanceProfileCredentialsProvider())
   // val credentialsProvider = new EnvironmentVariableCredentialsProvider()


    var clientBuilder = AmazonDynamoDBClientBuilder.standard().withCredentials(credentialsProvider)

    if (sys.env("DYNAMODB_URL") != "") {
      clientBuilder = clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(sys.env("DYNAMODB_URL"), "us-east-1"))
    }

    val dynamoDBClient = clientBuilder.build()

    val recordProcessorFactory = new RecordProcessorFactory(dynamoDBClient, tableName, batchSize.toInt, log)

    val workerId = String.valueOf(UUID.randomUUID)

    var kclConfig = new KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
      .withCommonClientConfig(new ClientConfiguration())
      .withRegionName("us-east-1")

    if (sys.env("DYNAMODB_URL") != "") {
      kclConfig = kclConfig.withDynamoDBEndpoint(sys.env("DYNAMODB_URL"))
    }
    if (sys.env("KINESIS_URL") != "") {
      kclConfig = kclConfig.withKinesisEndpoint(sys.env("KINESIS_URL"))
    }

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
