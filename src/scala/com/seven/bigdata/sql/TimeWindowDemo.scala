package com.seven.bigdata.sql

import com.seven.bigdata.config.SparkSessionConfig
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.functions.avg

/**
 * 基于时间的窗口分析
 */
object TimeWindowDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSessionConfig.getSparkSession("TimeWindowDemo")

    println(s"Running Spark Version ${spark.version}") //

    val stocksDF = spark.read
      .option("header","true")
      .option("inferschema","true")
      .csv("data/applestock.csv")

    stocksDF.printSchema()
    stocksDF.show()

    //找出2016年的股票交易数据(股票交易时间、当天收盘的价格)
    val stocks2016 = stocksDF.filter("year(Date)==2016")
    stocks2016.show()

    import spark.implicits._

    //计算平均值
    val tumblingWindowDS = stocks2016.groupBy(
      window(stocks2016.col("Date"),"1 week")
    ).agg(avg("Close").as("weekly_average"))

    print("计算平均值")
    tumblingWindowDS.show()

    //









  }

}
