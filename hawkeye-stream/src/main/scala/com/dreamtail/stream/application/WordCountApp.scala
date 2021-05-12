package com.dreamtail.stream.application

import com.dreamtail.stream.service.WordCountService
import com.dreamtail.stream.util.ScUtil
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("word_count")
  val sc = new SparkContext(sparkConf)
  ScUtil.put(sc)
  val wordCountService = new WordCountService
  val array = wordCountService.analysis()
  array.foreach(println)
  sc.stop()
  ScUtil.clear()

}
