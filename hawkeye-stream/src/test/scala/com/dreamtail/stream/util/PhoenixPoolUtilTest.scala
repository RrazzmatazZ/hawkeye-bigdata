package com.dreamtail.stream.util

import com.alibaba.fastjson.JSONObject
import org.junit.Test

import java.sql.{PreparedStatement, ResultSet, ResultSetMetaData}
import scala.collection.mutable.ListBuffer

class PhoenixPoolUtilTest {

  @Test
  def connect(): Unit = {
    val list: List[JSONObject] = queryList("select * from \"test_phoenix\" ")
    println(list)
  }

  def queryList(sql: String): List[JSONObject] = {
    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //获取数据库连接池
    val dsPool = PhoenixPoolUtil.getDataSource
    val conn = dsPool.getConnection
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行SQL语句
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while (rs.next()) {
      val userStatusJsonObj = new JSONObject()
      for (i <- 1 to rsMetaData.getColumnCount) {
        userStatusJsonObj.put(rsMetaData.getColumnName(i), rs.getObject(i))
      }
      rsList.append(userStatusJsonObj)
    }
    dsPool.evictConnection(conn)
    rsList.toList
  }
}
