package com.dreamtail.stream.util

import org.junit.Assert._
import org.junit.Test

class RedisUtilTest {

  @Test
  def connect(): Unit = {
    val instance = RedisUtil.getInstance
    assertEquals("PONG", instance.ping())
  }
}
