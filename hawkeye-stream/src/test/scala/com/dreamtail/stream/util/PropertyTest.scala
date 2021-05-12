package com.dreamtail.stream.util

import org.junit.Assert._
import org.junit.Test

class PropertyTest {

  @Test
  def testPropertyGet():Unit={
    val properties = PropertyUtil.load("log4j.properties")
    assertNotNull(properties.getProperty("log4j.rootCategory"))
  }

}
