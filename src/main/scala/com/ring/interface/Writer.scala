package com.ring.interface

import java.time.LocalDateTime

trait Writer {
  def push(json: String): Boolean
}
