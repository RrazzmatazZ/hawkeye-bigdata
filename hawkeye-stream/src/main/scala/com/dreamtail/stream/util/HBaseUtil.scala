package com.dreamtail.stream.util

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}


object HBaseUtil extends Serializable {

  var conn: Connection = _

  def getHBaseConn: Connection = synchronized {
    if (conn == null) {
      val properties = PropertyUtil.load()
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"))
      conn = ConnectionFactory.createConnection(conf)
    }
    conn
  }
}
