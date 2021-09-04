package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author zhoums
 * @date 2021/8/28 10:28
 * @version 1.0
 */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}
