package com.dreamtail.stream.util

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.util.Properties

object PhoenixPoolUtil {
  @transient private var datasource: HikariDataSource = _

  def getDataSource: HikariDataSource = synchronized {
    if (datasource == null) {
      val properties: Properties = PropertyUtil.load()
      val conf: HikariConfig = new HikariConfig(properties)
      datasource = new HikariDataSource(conf)
    }
    datasource
  }

}


