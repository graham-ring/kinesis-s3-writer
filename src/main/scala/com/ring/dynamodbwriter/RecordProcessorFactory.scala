package com.ring.dynamodbwriter

import org.apache.commons.logging.Log
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory

class RecordProcessorFactory(_client: AmazonDynamoDB, _table: String, _batchSize: Int, _log: Log) extends IRecordProcessorFactory {
  private val client = _client
  private val table = _table
  private val batchSize = _batchSize
  private val log = _log

  def createProcessor(): IRecordProcessor = {
  	val writer = new DynamoDBBatchWriter(client, table, batchSize, log)
		return new RecordProcessor(writer)
  }

}
