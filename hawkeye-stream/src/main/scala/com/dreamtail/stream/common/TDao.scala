package com.dreamtail.stream.common

import com.dreamtail.stream.util.{KafkaUtil, RedisUtil, ScUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

trait TDao extends Serializable {

  /**
   * 从指定文件路径读取生成RDD
   *
   * @param path 文件路径
   * @return
   */
  def readFile(path: String): RDD[String] = {
    ScUtil.take().textFile(path)
  }

  /**
   * 从kafka中读取数据至DStream
   *
   * @param ssc     spark StreamingContext
   * @param topic   消费主题·
   * @param groupId 分组id
   * @param offset  偏移量
   * @return
   */
  def readKafkaStreamAutoOffset(ssc: StreamingContext, topic: String, groupId: String):
  InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtil.getKafkaStream(topic, ssc, groupId, true)
  }

  /**
   * 从kafka中读取数据至DStream，基于redis维护偏移量
   *
   * @param ssc     spark StreamingContext
   * @param topic   消费主题·
   * @param groupId 分组id
   * @return
   */
  def readKafkaStreamRedisOffset(ssc: StreamingContext, topic: String, groupId: String):
  InputDStream[ConsumerRecord[String, String]] = {
    val offset = RedisUtil.getRedisOffset(topic, groupId)
    var ds: InputDStream[ConsumerRecord[String, String]] = null
    if (offset != null && offset.nonEmpty) {
      ds = KafkaUtil.getKafkaStreamWithOffset(topic, ssc, offset, groupId)
    } else {
      ds = KafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    ds
  }

}
