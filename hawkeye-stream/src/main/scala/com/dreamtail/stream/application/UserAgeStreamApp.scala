package com.dreamtail.stream.application

import com.dreamtail.stream.service.{UserAgeStreamService, UserStreamService}
import com.dreamtail.stream.util.SscUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserAgeStreamApp extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("user_stream")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))
  try {
    SscUtil.put(ssc)
    val streamService = new UserAgeStreamService
    streamService.analysis()
    ssc.start()
    ssc.awaitTermination()
  }
  finally {
    SscUtil.clear()
  }
}
