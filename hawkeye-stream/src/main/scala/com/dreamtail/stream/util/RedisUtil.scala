package com.dreamtail.stream.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.util

/**
 * redis common usage
 *
 * @author xdq
 */
object RedisUtil {

  private var jedisPool: JedisPool = _

  private def build(): Unit = {
    val prop = PropertyUtil.load()
    val host = prop.getProperty("redis.host")
    val port = prop.getProperty("redis.port")
    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig {
      setMaxTotal(100) //最大连接数
      setMaxIdle(20) //最大空闲
      setMinIdle(20) //最小空闲
      setBlockWhenExhausted(true) //忙碌时是否等待
      setMaxWaitMillis(5000) //忙碌时等待时长
      setTestOnBorrow(true) //每次获得连接的进行测试
    }
    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

  def getInstance: Jedis = this.synchronized {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }

  /**
   * 从redis读取kafka对应消费主题的偏移量
   *
   * @param topic   主题名称
   * @param groupId 消费组id
   * @return
   */
  def getRedisOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    //获取客户端连接
    val jedis: Jedis = this.getInstance
    //拼接操作redis的key[offset:topic:groupId]
    val offsetKey = "offset:" + topic + ":" + groupId
    //获取当前消费者组消费的主题对应的分区以及偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭客户端
    jedis.close()

    import scala.collection.JavaConverters._
    //将java的map转换为scala的map
    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) =>
        println("读取分区偏移量：" + partition + ":" + offset)
        (new TopicPartition(topic, partition.toInt), offset.toLong)
    }.toMap
    oMap
  }

  /**
   * 保存对应消费主题的当前偏移量
   *
   * @param topic        主题
   * @param groupId      消费组id
   * @param offsetRanges 偏移量信息
   */
  def putRedisOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    //拼接redis中操作偏移量的key
    var offsetKey = "offset:" + topic + ":" + groupId
    //定义java的map集合，用于存放每个分区对应的偏移量
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    //对offsetRanges进行遍历，将数据封装offsetMap
    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString, untilOffset.toString)
      println("保存分区" + partitionId + ":" + fromOffset + "----->" + untilOffset)
    }
    val jedis: Jedis = this.getInstance
    jedis.hmset(offsetKey, offsetMap)
    jedis.close()
  }
}
