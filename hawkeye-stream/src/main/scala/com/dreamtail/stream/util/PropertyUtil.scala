package com.dreamtail.stream.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

object PropertyUtil {

  var prop: Properties = _;

  /**
   * 读取环境配置
   *
   * @param path 环境配置文件目录
   * @return
   */
  def load(): Properties = synchronized {
    if (prop == null) {
      prop = new Properties()
      prop.load(new InputStreamReader(
        Thread.currentThread().getContextClassLoader.getResourceAsStream("application.properties"),
        StandardCharsets.UTF_8))
    }
    prop
  }

  def load(path: String): Properties = {
    val propFromPath = new Properties()
    propFromPath.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(path),
      StandardCharsets.UTF_8))
    propFromPath
  }

}
