package com.dreamtail.stream.service

import com.dreamtail.stream.common.TService
import com.dreamtail.stream.util.SscUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class QueueStreamService extends TService {
  /**
   * 执行分析服务
   *
   * @return
   */
  override def analysis(): Any = {
    val rddQueue = new mutable.Queue[RDD[Int]]()
    val ssc = SscUtil.take()
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()

    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
  }
}
