package com.dreamtail.stream.util

import org.apache.hadoop.hbase.TableName
import org.junit.Test

class HBaseUtilTest {

  @Test
  def connectTable(): Unit = {
    val connection = HBaseUtil.getHBaseConn
    val table = connection.getTable(TableName.valueOf("TEST_USER"))
    println(table.getName)
  }

}
