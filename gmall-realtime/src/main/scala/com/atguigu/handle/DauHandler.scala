package com.atguigu.handle

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author zhoums
 * @date 2021/8/28 15:22
 * @version 1.0
 */
object DauHandler {

  /**
   * 批次内去重
   * @param filterByRedisDStream
   */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {

    //1.将数据转换为k,v ((mid,logDate),log) 加上时间作为key 防止零点漂移
    val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.mapPartitions(partition => {
      partition.map(startUpLog => {
        ((startUpLog.mid, startUpLog.logDate), startUpLog)
      })
    })

    //2.将相同的key聚合到同一个分区中
    val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()

    //3.排序并取第一条数据,(因为只需要一条就够了)
    val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //4.数据扁平化，拆散
    val value: DStream[StartUpLog] = midAndDateToLogListDStream.flatMap(_._2)

    value
  }


  /**
   * 批次间去重
   * @param startUpLogDStream
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    //方法一
//    val value: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
//      //a.创建Jedis连接
//      val jedis: Jedis = new Jedis("hadoop105", 6379)
//
//      //b.查询redis中保存的mid
//      //redis的key
//      val redisKey: String = "DAU:" + startUpLog.logDate
//      val mids: util.Set[String] = jedis.smembers(redisKey)
//
//      //c.拿当前批次的mid与redis中已经存在的mid做对比------- 有：过滤掉  没有：保留
//      val bool: Boolean = mids.contains(startUpLog.mid)
//      jedis.close()
//      //filter是true保留，false舍掉。所以此处需要取反
//      !bool
//
//    })

//    //方法二：在每个分区下创建连接，减少连接数
//    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
//
//      //a.创建Jedis连接
//      val jedis: Jedis = new Jedis("hadoop105", 6379)
//
//      val logs: Iterator[StartUpLog] = partition.filter(startUpLog => {
//        //b.查询redis中保存的mid
//        //redis的key
//        val redisKey: String = "DAU:" + startUpLog.logDate
//        val mids: util.Set[String] = jedis.smembers(redisKey)
//
//        //c.拿当前批次的mid与redis中已经存在的mid做对比------- 有：过滤掉  没有：保留
//        val bool: Boolean = mids.contains(startUpLog.mid)
//        jedis.close()
//        //filter是true保留，false舍掉。所以此处需要取反
//        !bool
//      })
//      logs
//    })

    //方案三：每个批次下获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //执行在driver端，一个批次执行一次
      //a.创建Jedis连接(driver)
      val jedis: Jedis = new Jedis("hadoop105", 6379)

      //b.查询redis中保存的mid(driver)
      //redis的key
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //c.将driver端的数据广播到executor端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      val midFilterRDD: RDD[StartUpLog] = rdd.filter(startUpLog => {
        //(executor)
        !midBC.value.contains(startUpLog.mid)
      })

      //关闭连接
      jedis.close()
      midFilterRDD
    })

    value
  }


  /**
   * 将去重后的mid保存到reids中
   *
   * @param startUpLogDStream
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{

      rdd.foreachPartition(partition=>{
        //a.创建Jedis连接
        val jedis: Jedis = new Jedis("hadoop105", 6379)
        partition.foreach(startUpLog=>{
          //redis的key
          val redisKey: String = "DAU:"+startUpLog.logDate
          //b.将数据(mid)保存至redis
          jedis.sadd(redisKey,startUpLog.mid)
        })
        jedis.close()
      })
    })
  }



}
