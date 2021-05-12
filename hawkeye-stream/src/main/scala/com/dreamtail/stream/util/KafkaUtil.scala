package com.dreamtail.stream.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaUtil {

  private val kafkaParam = collection.mutable.Map(
    //用于初始化链接到集群的地址
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertyUtil.load().getProperty("kafka.broker.list"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    ConsumerConfig.GROUP_ID_CONFIG -> Nil,
    //latest自动重置偏移量为最新的偏移量
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //false需要手动维护kafka偏移量，框架会基于redis去维护
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
  )

  /**
   * 在对Kafka数据进行消费的时候，指定消费者组
   *
   * @param topic      消费主题
   * @param ssc        spark StreamingContext
   * @param groupId    分组id
   * @param autoCommit offset是否自动维护，false为默认值，需要redis维护
   * @return InputDStream
   */
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String, autoCommit: java.lang.Boolean = false): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    kafkaParam(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) = autoCommit
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    dStream
  }

  /**
   * 从指定的偏移量位置读取数据
   *
   * @param topic      消费主题
   * @param ssc        spark StreamingContext
   * @param offsets    偏移量
   * @param groupId    分组id
   * @param autoCommit offset是否自动维护，false为默认值，需要redis维护
   * @return
   */
  def getKafkaStreamWithOffset(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId: String, autoCommit: java.lang.Boolean = false): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    kafkaParam(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) = autoCommit
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
    dStream
  }


}
