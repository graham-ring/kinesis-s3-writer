package com.ring.dynamodbwriter

import java.util.{Map => JMap, List => JList}
import org.apache.commons.logging.Log
import com.ring.interface.Writer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.datamodeling.JsonMarshaller
import com.amazonaws.services.dynamodbv2.datamodeling.marshallers.{MapToMapMarshaller, NumberToNumberMarshaller, StringToStringMarshaller}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutRequest, WriteRequest}
import scala.collection.mutable.{ListMap, Buffer} 
import scala.collection.JavaConversions._

class DynamoDBBatchWriter(_client: AmazonDynamoDB, _table: String, _batchSize: Int, _log: Log) extends Writer {
  private val client = _client
  private val table = _table
  private val batchSize = _batchSize
  private val log = _log

  private val items = Buffer[WriteRequest]()

  def push(js: String): Boolean = {
    addItem(writeRequest(js))

    if (full()) {
      write()
      return true
    }

    return false
  }

  private def addItem(req: WriteRequest) = {
    println(s"$req")
    items += req
  }

  private def full(): Boolean = {
    return (items.length >= batchSize)
  }

  private val mapClass = classOf[JMap[String, Any]]

  private def writeRequest(js: String):WriteRequest = {
    val map = new JsonMarshaller().unmarshall(mapClass, js)
    return new WriteRequest(new PutRequest(attributeValueMap(map)))
  }

  private val maxTries = 3

  private def write(tries: Int):Unit = {
    val itemsToWrite = mapAsJavaMap(Map[String, JList[WriteRequest]](table -> items))
    items.clear()

    try {
      if (tries > 0) {
        if (tries >= maxTries) {
          throw new Exception("exceeded max tries")
        }

        Thread.sleep(1000) //SHOULD BE EXPONENTIAL
      }

      val result = client.batchWriteItem(itemsToWrite)
      val unprocessedItems = result.getUnprocessedItems().get(table)
      if (unprocessedItems.length > 0) {
        //LOG
        unprocessedItems.map(addItem)
        write(tries + 1)
      }
    }
    catch {
      case t: Throwable =>
        log.error("Caught throwable while processing data.", t)
    }
  }

  private def write():Unit = write(0)  

  private def attributeValueMap(m: JMap[String, Any]):JMap[String, AttributeValue] = {
    return m mapValues attributeValue
  }

  private def attributeValueList(l: JList[Any]):JList[AttributeValue] = {
    return l.map(attributeValue)
  }

  private def attributeValue(v: Any):AttributeValue = v match {
    case b: Boolean => new AttributeValue().withBOOL(b)
    case s: String => new AttributeValue().withS(s)
    case i: Int => new AttributeValue().withN(i.toString)
    case f: Float => new AttributeValue().withN(f.toString)
    case m: JMap[String, Any] => new AttributeValue().withM(attributeValueMap(m))
    case l: JList[Any] => new AttributeValue().withL(attributeValueList(l))
    case _ => new AttributeValue().withNULL(true)
  }

}
