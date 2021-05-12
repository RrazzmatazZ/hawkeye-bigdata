package com.dreamtail.stream.dao

import com.dreamtail.stream.common.TDao
import com.dreamtail.stream.util.ScUtil
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao {

  def makeRDD(): RDD[String] = {
    ScUtil.take().makeRDD(List("hello world", "hello scala"))
  }

}
