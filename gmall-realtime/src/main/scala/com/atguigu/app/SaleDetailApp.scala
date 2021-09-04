package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import collection.JavaConverters._

/**
 * @author zhoums
 * @date 2021/9/3 14:31
 * @version 1.0
 */
object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建sparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.获取kafka中的数据
    val kafkaOrderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val kafkaDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将两个流的数据转为样例类,并且为kv类型，k-》指的是join关联条件orderId,v是数据本身
    val orderInfoDStream = kafkaOrderDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id,orderInfo)
      })
    })

    val orderDetailDStream = kafkaDetailDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id,orderDetail)
      })
    })

//    orderInfoDStream.print()
//    orderDetailDStream.print()

    //5.使用fullouterjoin防止join不上的数据丢失
    val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.操作数据
    val noUserDStream: DStream[SaleDetail] = joinDStream.mapPartitions(partition=>{
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop105", 6379)
      //创建list集合用于存放saleDetail样例类
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      partition.foreach{
        case (orderId, (orderOpt, detailOpt))=>{
          //redisKey orderInfoKey
          val orderInfoKey = "orderInfo:"+orderId
          //orderDetailKey
          val orderDetailKey = "orderDetail:" + orderId

          //a.判断是否有orderInfo数据
          if (orderOpt.isDefined) {
            //有orderInfo
            val orderInfo: OrderInfo = orderOpt.get
            //b.判断orderDetail是否存在
            if (detailOpt.isDefined) {
              //orderDetail存在
              val orderDetail: OrderDetail = detailOpt.get
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(saleDetail)
            }

            //c.将orderInfo数据保存到redis
            //将样例类转为字符串
            //无法使用java的Json.toJSONString
            implicit val formats=org.json4s.DefaultFormats
            val orderInfoJson: String = Serialization.write(OrderInfo)
            jedis.set(orderInfoKey,orderInfoJson)
            //设置过期时间，（企业里问员工）
            jedis.expire(orderInfoKey,30)

            //d.去对方缓存(orderDetail)中查询是否有可以关联上的数据
            //判断对方缓存中是否有可以关联的key
            if (jedis.exists(orderDetailKey)){
              val orderDetails: util.Set[String] = jedis.smembers(orderDetailKey)
              for (elem <- orderDetails.asScala) {
                //将查询出来的json串转为样例类
                val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
                val saleDetail = new SaleDetail(orderInfo, orderDetail)
                details.add(saleDetail)
              }
            }
          }else{
            //没有orderInfo数据
            if (detailOpt.isDefined) {
              //a.拿到orderDetail数据
              val orderDetail: OrderDetail = detailOpt.get

              //判断是否有可以匹配的orderInfo数据
              if(jedis.exists(orderInfoKey)){
                //存在orderInfo
                //a.1 获取orderInfo数据  因为info -》detail 1-》n 所以上面用smembers，此处用get
                val orderInfoJson: String = jedis.get(orderInfoKey)

                //a.2 orderInfoJson转化为样例类
                val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])

                //a.3将join上的数据添加到SaleDetail样例类里
                val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
                details.add(saleDetail)
              }else{
                //不存在orderInfo
                //将orderDetail写入redis缓存
                //a.首先，将样例类转为json orderDetailJson
                val orderInfoJson: String = Serialization.write(orderDetail)(org.json4s.DefaultFormats)

                //b.写入redis
                jedis.set(orderDetailKey,orderInfoJson)

                //c.设置过期时间
                jedis.expire(orderDetailKey,30)
              }

            }
          }
        }
      }

      //jedis关闭连接
      jedis.close()
      details.asScala.toIterator
    })

    //7.补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions(partition => {
      //a.获取jedis连接
      val jedis: Jedis = new Jedis("hadoop105", 6379)
      //b.查取redis库
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //根据key获取数据
        val userInfoKey: String = "userInfo:" + saleDetail.user_id
        val userInfoJson: String = jedis.get(userInfoKey)
        //将数据userInfoJson转换为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])

        //c.数据补全
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      //关闭连接
      jedis.close()
      details
    })

    //8.将数据写入es
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //索引名
        val indexName = GmallConstants.ES_DETAIL_INDEXNAME+"-"+sdf.format(new Date(System.currentTimeMillis()))

        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })

        MyEsUtil.insertBulk(indexName,list)
      })
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
