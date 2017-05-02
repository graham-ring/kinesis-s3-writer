package com.ring.parquetwriter

import com.ring.interface.Writer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory

class RecordProcessorFactory(_writer: Writer) extends IRecordProcessorFactory {

  private val writer: Writer = _writer

  def createProcessor(): IRecordProcessor = {
		return new RecordProcessor(writer)
  }

}
