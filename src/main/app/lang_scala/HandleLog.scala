package lang_scala

import java.sql.DriverManager
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 动态读取日志
 * 筛选之后存入数据库
 **/
object HandleLog extends App {
  Logger.getLogger("org").setLevel(Level.ERROR) //配置日志

  val conf = new SparkConf().setMaster("local[*]").setAppName("logHandle")
  val ssc = new StreamingContext(conf, Seconds(2))
  val session = SparkSession
    .builder()
    .appName("log")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  val input = "./logs/" //日志存储路径
  val textStream = ssc.textFileStream(input)
  val prop = new Properties()
  prop.setProperty("user", "root")
  prop.setProperty("password", "sys123")
  val wcStream = textStream.foreachRDD(_.
    map(line => {
      var level = line.split("]")(0).replace("[", "") //截取每条日志的等级信息
      var date = line.split(":")(1) //截取日志的日期信息
      var info = line.split("\t")(1) //描述信息
      println("(" + level + "," + date + "," + info + ")")
      (level, info, date) //返回每条日志的信息
    })
    .filter(_._1.equals("error")) //筛选出日志等级为error
    .toDF("level","info","date")
    .write
    .mode(SaveMode.Append)
    .jdbc("jdbc:mysql://localhost:3306/test", "logs", prop)
  )
  ssc.start()
  ssc.awaitTermination()
}