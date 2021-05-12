package com.dreamtail.stream.service

import com.dreamtail.stream.common.TService
import com.dreamtail.stream.dao.UserStreamDao
import com.dreamtail.stream.util.{RedisUtil, SscUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import java.time.LocalDate

class UserAgeStreamService extends TService {
  private val topic: String = "sync_user_age"
  private val groupId: String = "group1"
  private val dao = new UserStreamDao()

  /**
   * 执行分析服务
   *
   * @return
   */
  override def analysis(): Any = {
    val ssc = SscUtil.take()
    val ds = dao.readKafkaStreamRedisOffset(ssc, topic, groupId)
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
    val offsetDStream: DStream[ConsumerRecord[String, String]] = ds.transform {
      rdd => {
        //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val ageStream: DStream[Int] = offsetDStream.map(item => {
      RedisUtil.putRedisOffset(topic, groupId, offsetRanges)
      val bornYear: String = item.value().substring(6, 10)
      val currentYear = LocalDate.now().getYear
      currentYear - bornYear.toInt
    })
    
    ageStream.print()
  }
}
