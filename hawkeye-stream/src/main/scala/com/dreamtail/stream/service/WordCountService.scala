package com.dreamtail.stream.service

import com.dreamtail.stream.common.TService
import com.dreamtail.stream.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * 服务层
 */
class WordCountService extends TService {

  private val wordCountDao = new WordCountDao()

  // 数据分析
  def analysis(): Array[(String, Int)] = {
    val lines = wordCountDao.makeRDD()
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val array: Array[(String, Int)] = wordToSum.collect()
    array
  }
}
