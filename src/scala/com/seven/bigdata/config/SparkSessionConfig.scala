package com.seven.bigdata.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Spark的上下文获取
  *
  * @Author: liduo
  * @Version 1.0
  * @Date: 2019/3/30 16:54
  */
object SparkSessionConfig {

  /**
    *
    * @param appName
    * @param master
    * @return
    */
  def getSparkSession (appName:String,master:String = "local") = {

    val sparkConfig = new SparkConf()
      .setAppName(appName)
      .setMaster(master)

    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConfig)
      .getOrCreate()

//    println(s"Running Spark Version ${spark.version}")

    //调整开发输出的日志信息
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    spark
  }

}
