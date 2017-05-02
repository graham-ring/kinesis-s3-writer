package com.ring.interface

import java.time.LocalDateTime

trait Writer {
  def newWithPrefix(prefix: String): Writer
  def push(json: String, time: LocalDateTime): Boolean
  def flush()
}
