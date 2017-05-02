package com.ring.parquetwriter

import java.lang.String
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.List
import com.ring.interface.Writer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

class RecordProcessor(_writer: Writer) extends IRecordProcessor {

  private var writer: Writer = _writer
  private var lastProcessedRecord: Record = _

  def initialize(shardId: String) = {
    writer = writer.newWithPrefix(shardId)
  }

  def processRecords(records: List[Record], checkpointer: IRecordProcessorCheckpointer) = {
    val iterator = records.iterator();
		 
    while (iterator.hasNext()) {
      processRecord(iterator.next(), checkpointer)
    }
  }

  private val zoneId = ZoneId.systemDefault()

  def processRecord(record: Record, checkpointer: IRecordProcessorCheckpointer) = {
    val json = new String(record.getData().array())
    val instant = record.getApproximateArrivalTimestamp().toInstant()
    val time = LocalDateTime.ofInstant(instant, zoneId)

    if(!writer.push(json, time)) {
      flushWriter(checkpointer)
      writer.push(json, time)
    }

    lastProcessedRecord = record
  }

  def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason) = {
 		if (reason == ShutdownReason.TERMINATE) {
      flushWriter(checkpointer)
    }
  }

  def flushWriter(checkpointer: IRecordProcessorCheckpointer) = {
    writer.flush()
    checkpointer.checkpoint(lastProcessedRecord)
  }

}
