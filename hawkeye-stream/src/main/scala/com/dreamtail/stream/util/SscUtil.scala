package com.dreamtail.stream.util

import org.apache.spark.streaming.StreamingContext

/**
 * Spark StreamingContext in local Thread
 *
 * @author xdq
 */
object SscUtil {
  private val sscLocal = new ThreadLocal[StreamingContext]()

  /**
   * 保存StreamingContext到当前Thread
   *
   * @param sc StreamingContext
   */
  def put(sc: StreamingContext): Unit = {
    sscLocal.set(sc)
  }

  /**
   * 读取当前Thread下的StreamingContext
   *
   * @return
   */
  def take(): StreamingContext = {
    sscLocal.get()
  }

  /**
   * 清楚当前Thread下的StreamingContext
   */
  def clear(): Unit = {
    sscLocal.remove()
  }
}
