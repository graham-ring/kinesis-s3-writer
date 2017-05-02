package com.ring.parquetwriter

import java.nio.file.{FileSystems, FileSystem}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import com.ring.interface.Writer
import com.ring.utils.GenericJsonDecoder


class BufferedWriter(_schema: Schema, _bucket: String, _prefix: String) extends Writer {
  private val schema = _schema
  private val bucket = _bucket
  private val prefix = _prefix

  private val fs: FileSystem = FileSystems.getDefault()
  private val decoder = new GenericJsonDecoder(schema)
  private var writer: AvroParquetWriter[GenericRecord] = _

  private val parquetBlockSize = 256 * 1024 * 1024;
  private val parquetPageSize = 64 * 1024;

  def this(schema: Schema, bucket: String) = this(schema, bucket, "_")  

  def newWithPrefix(newPrefix: String): Writer = {
    return new BufferedWriter(schema, bucket, newPrefix)
  }

  def push(json: String, time: LocalDateTime): Boolean = {
    if (open && (writerExpired(time) || writerFull())) {
      return false
    }

    if (!open) { 
      initWriter(time)
    }

    val record = decoder.decode(json)
    writer.write(record)

    return true
  }

  private val instanceProfileCredentialsProvider = new InstanceProfileCredentialsProvider()
  private val s3 = new AmazonS3Client(instanceProfileCredentialsProvider)

  def flush() = {
    writer.close()

    val localFile = fs.getPath(localFilePath()).toFile()
    s3.putObject(bucket, remoteFilePath(), localFile)

    // wipe localDir
    // f.delete()
    // val crc = s".$openFile.crc"
    // val crcf = fs.getPath(filePath(crc)).toFile()
    // crcf.delete()
    open = false
  }

  private var open: Boolean = false
  private var openTime: LocalDateTime = _

  private def initWriter(time: LocalDateTime) = {
    openTime = time
    writer = new AvroParquetWriter(new Path(localFilePath()), schema, CompressionCodecName.SNAPPY, parquetBlockSize, parquetPageSize);
    open = true
  }

  private def writerExpired(time: LocalDateTime): Boolean = {
    return (time.getLong(ChronoField.EPOCH_DAY) > openTime.getLong(ChronoField.EPOCH_DAY)) || (time.getHour() > openTime.getHour())
  }

  //private val sizeThreshold: Int = 100000

  private def writerFull(): Boolean = {
    // is writer buffer size >= sizeThreshold
    return false
  }

  private val localDir = "tmp"

  private def localFilePath(): String = {
    val hashCode = openTime.hashCode()
    return s"$localDir/$hashCode"
  }

  private val pathFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH");

  private def remoteFilePath(): String = {
    val path = openTime.format(pathFormatter)
    val seconds = (openTime.getMinute() * 60) + openTime.getSecond()
    val fileName = s"$prefix-$seconds"
    return s"$path/$fileName"
  }

}
