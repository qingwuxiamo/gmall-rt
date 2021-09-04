package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @author zhoums
 * @date 2021/8/28 10:39
 * @version 1.0
 */
object DauApp {

  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.通过kafka工具类消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将kafka读取的json格式的数据转为样例类，补齐缺少的字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转化为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        //补全字段  yyyy-MM-dd HH
        val times: String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = times.split(" ")(0)
        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })

    //流多次使用可以使用cache优化
    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    //5.批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)


    //经过批次间去重的数据条数
    filterByRedisDStream.cache()
    filterByRedisDStream.count().print()

    //6.批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    //经过批次内去重的数据条数
    filterByGroupDStream.cache()
    filterByGroupDStream.count().print()

    //7.将去重后的数据保存到redis中
    DauHandler.saveMidToRedis(filterByGroupDStream)


    //8.将最终去重后的明细数据保存道HBase
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop105,hadoop106,hadoop107:2181"))
    })

//    //测试消费kafka数据
//    kafkaDStream.foreachRDD(
//      rdd=>rdd.foreachPartition(
//        partition=>partition.foreach(
//          record=> println(record.value())
//        )
//      )
//    )

    ssc.start()

    ssc.awaitTermination()

  }

}
