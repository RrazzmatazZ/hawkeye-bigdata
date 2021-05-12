package com.dreamtail.stream.service

import com.alibaba.fastjson.JSON
import com.dreamtail.stream.bean.User
import com.dreamtail.stream.common.TService
import com.dreamtail.stream.dao.UserStreamDao
import com.dreamtail.stream.util.{HBaseUtil, SscUtil}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}


class UserStreamService extends TService {

  private val topic: String = "user"
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

    //对数据进行清洗
    val mapStream = offsetDStream.map(item => {
      val jsonStr: String = item.value()
      if (jsonStr != "") {
        println(jsonStr)
      }
      val user = JSON.parseObject(jsonStr, classOf[User])
      user
    })

    mapStream.foreachRDD(
      rdd => {
        rdd.foreach(
          (item: User) => {
            val connection = HBaseUtil.getHBaseConn
            val table = connection.getTable(TableName.valueOf("TEST_USER"))
            val id = item.id
            val put = new Put(Bytes.toBytes(id))
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(item.name))
            put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("sex"), Bytes.toBytes(item.sex))
            table.put(put)
          }
        )
      }
    )

    //    //数据入库
    //    import org.apache.phoenix.spark._
    //    mapStream.foreachRDD(
    //      (rdd: RDD[User]) => {
    //        rdd.saveToPhoenix(
    //          "TEST_USER",
    //          Seq("ID", "NAME", "SEX"),
    //          new Configuration,
    //          Some("hadoop242,hadoop248,hadoop249:2181")
    //        )
    //
    //        RedisUtil.putRedisOffset(topic, groupId, offsetRanges)
    //      }
    //    )

    //    mapStream.foreachRDD(
    //      rdd => {
    //        rdd.foreach(
    //          item => {
    //            HBaseUtils.
    //          }
    //        )
    //      }
    //    )
  }
}