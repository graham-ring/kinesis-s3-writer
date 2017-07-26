package com.ring.dynamodbwriter

import java.lang.String
import java.util.List
import com.ring.interface.Writer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

class RecordProcessor(_writer: Writer) extends IRecordProcessor {

  private var writer: Writer = _writer

  def initialize(shardId: String) = {
    println(shardId)
    //writer = writer.newWithPrefix(shardId)
  }

  def processRecords(records: List[Record], checkpointer: IRecordProcessorCheckpointer) = {
    val iterator = records.iterator();
		 
    while (iterator.hasNext()) {
      processRecord(iterator.next(), checkpointer)
    }
  }

  def processRecord(record: Record, checkpointer: IRecordProcessorCheckpointer) = {
    val json = new String(record.getData().array())

    if(writer.push(json)) {
      checkpointer.checkpoint(record)
    }
  }

  def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason) = {
 		// if (reason == ShutdownReason.TERMINATE) {
   //    flushWriter(checkpointer)
   //  }
  }

}
