package com.dreamtail.stream.util

import org.apache.spark.SparkContext

/**
 * SparkContext in local Thread
 *
 * @author xdq
 */
object ScUtil {
  private val scLocal = new ThreadLocal[SparkContext]()

  /**
   * 保存SparkContext到当前Thread
   *
   * @param sc sparkContext
   */
  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  /**
   * 读取当前Thread下的SparkContext
   *
   * @return
   */
  def take(): SparkContext = {
    scLocal.get()
  }

  /**
   * 清楚当前Thread下的SparkContext
   */
  def clear(): Unit = {
    scLocal.remove()
  }
}
