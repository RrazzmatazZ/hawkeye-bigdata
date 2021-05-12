package com.dreamtail.stream.application

import com.dreamtail.stream.service.UserStreamService
import com.dreamtail.stream.util.SscUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserStreamApp extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("user_stream")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))
  try {
    SscUtil.put(ssc)
    val streamService = new UserStreamService
    streamService.analysis()
    ssc.start()
    ssc.awaitTermination()
  }
  finally {
    SscUtil.clear()
  }
}
