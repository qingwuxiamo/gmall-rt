package com.atguigu.bean

/**
 * @author zhoums
 * @date 2021/8/28 10:38
 * @version 1.0
 */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long)
