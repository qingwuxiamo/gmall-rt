package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

/**
 * @author zhoums
 * @date 2021/9/3 14:31
 * @version 1.0
 */
object UserInfoApp {

  def main(args: Array[String]): Unit = {

    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2.创建sparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.获取kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //4.将数据转为样例类
    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })

//    userInfoDStream.print()

    //5.将数据写入redis
    userInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //a.建立jedis连接
        val jedis: Jedis = new Jedis("hadoop105", 6379)
        //b.写入库
        partition.foreach(userInfo =>{
          //设立redisKey
          val userInfoKey: String = "userInfo:" + userInfo.id
          //将样例类转为Json字符串
          implicit val formats=org.json4s.DefaultFormats
          val userInfoJson: String = Serialization.write(userInfo)
          jedis.set(userInfoKey,userInfoJson)
        })

        //c.关闭jedis
        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
