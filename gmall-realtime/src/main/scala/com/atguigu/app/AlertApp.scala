package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._
/**
 * @author zhoums
 * @date 2021/9/3 9:12
 * @version 1.0
 */
object AlertApp {

  def main(args: Array[String]): Unit = {
    //TODO 1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //TODO 2.初始化SparkStreaming
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //3.获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //4.将json转为样例类，并补全字段,返回kv格式数据（因为开窗后需要groupByKey）
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToEventLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转为样例类
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //补全字段
        val times: String = sdf.format(new Date(eventLog.ts))
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        (eventLog.mid, eventLog)
      })
    })

    //5.开窗（5分钟）
    val midToLogWindowDStream: DStream[(String, EventLog)] = midToEventLogDStream.window(Minutes(5))

    //6.将相同mid聚合到一起
    val midToIterLogDStream: DStream[(String, Iterable[EventLog])] = midToLogWindowDStream.groupByKey()

    //7.根据条件筛选数据 没有浏览商品 -》 领优惠券  -》 符合行为的用户是否大于等于三
    val boolToCouponAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterLogDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        //java的set，es支持java，样例类也需要的是javaset
        //创建set集合用于存放用户id
        val uIds: util.HashSet[String] = new util.HashSet[String]()
        //创建set集合用于存放领优惠券所涉及到的商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建List集合用于存放用户涉及到的事件id
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //定义标志位，判断是否为浏览商品行为
        var flag: Boolean = true

        //遍历迭代器中的数据
        breakable {
          for (elem <- iter) {
            //将用户涉及到的行为存放到集合中
            events.add(elem.evid)
            //判断用户是否有浏览商品行为
            if ("clickItem".equals(elem.evid)) {
              //有,则跳出当前循环

              flag = false
              break()
            } else if ("coupon".equals(elem.evid)) {
              //领优惠券行为
              //存放用户id
              uIds.add(elem.uid)
              //存放所涉及的商品id
              itemIds.add(elem.itemid)
            }
          }
        }

        //生成疑似预警日志
        (uIds.size() >= 3 && flag, CouponAlertInfo(mid, uIds, itemIds, events, System.currentTimeMillis()))
      }
    })


    //8.生成预警日志
    val couponAlertDStream: DStream[CouponAlertInfo] = boolToCouponAlertInfoDStream.filter(_._1).map(_._2)
    couponAlertDStream.print()

    //9.将预警日志写入es
    couponAlertDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val tuples: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
          //1分钟展示一次
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME, tuples)
      })
    }
    )


    //TODO 10.启动SparkStreamingContext
    ssc.start()
    // 将主线程阻塞，主线程不退出
    ssc.awaitTermination()
  }

}
