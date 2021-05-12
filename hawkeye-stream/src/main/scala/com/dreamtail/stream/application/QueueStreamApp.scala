package com.dreamtail.stream.application

import com.dreamtail.stream.service.QueueStreamService
import com.dreamtail.stream.util.SscUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QueueStreamApp extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("queue_stream")
  val ssc = new StreamingContext(sparkConf, Seconds(3))
  try {
    SscUtil.put(ssc)
    val streamService = new QueueStreamService
    streamService.analysis()
    ssc.awaitTermination()
  }
  finally {
    SscUtil.clear()
  }
}
