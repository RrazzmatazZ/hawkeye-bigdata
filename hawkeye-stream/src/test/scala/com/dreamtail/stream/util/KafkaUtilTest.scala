package com.dreamtail.stream.util

import com.dreamtail.stream.bean.User
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}
import org.junit.Test

import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer

class KafkaUtilTest {

  /**
   * 生产word推送到kafka
   */
  @Test
  def mockWordCountData(): Unit = {
    val prop = new Properties() {
      put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop242:9092,hadoop248:9092,hadoop249:9092")
      put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    }
    val producer = new KafkaProducer[String, String](prop)
    while (true) {
      createWord().foreach(
        (data: String) => {
          // 向Kafka中生成数据
          val record = new ProducerRecord[String, String]("word_count", data)
          producer.send(record)
          println(data)
        }
      )
      Thread.sleep(2000)
    }

    def createWord(): ListBuffer[String] = {
      val mealList = ListBuffer[String]()
      val fruits = ListBuffer[String]("苹果", "香蕉", "橙子")
      val meats = ListBuffer[String]("猪肉", "鸡肉", "牛肉")
      for (i <- 1 to 3) {
        val fruit = fruits(new Random().nextInt(3))
        val meat = meats(new Random().nextInt(3))
        mealList.append(s"${fruit} ${meat}")
      }
      mealList
    }
  }

  @Test
  def mockUser(): Unit = {
    val prop = new Properties() {
      put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop242:9092,hadoop248:9092,hadoop249:9092")
      put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    }

    implicit val f: DefaultFormats.type = org.json4s.DefaultFormats

    val producer = new KafkaProducer[String, String](prop)
    val user = createUser()
    val record = new ProducerRecord[String, String]("user", Serialization.write(user))
    producer.send(record)
    Thread.sleep(2000)
  }

  def createUser(): User = {
    val user1 = User("420621199806208765", "zs", "male")
    user1
  }

  @Test
  def testJSONFormat(): Unit = {
    //数据格式：user:{"name": "zs","age": 10}
    val json = "{\"id\": \"420621199806208765\",\"name\": \"zs\",\"sex\": \"male\"}"

    //导隐式函数
    implicit val f: DefaultFormats.type = org.json4s.DefaultFormats
    /** ************************* json字符串->对象 ************************************** */

    val user: User = JsonMethods.parse(json).extract[User]
    print(user)

    /** ************************* 对象->json字符串 ************************************** */
    val userJStr: String = Serialization.write(user)
    println(userJStr)
  }

}
