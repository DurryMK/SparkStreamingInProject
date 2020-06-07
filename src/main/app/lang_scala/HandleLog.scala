package lang_scala

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 动态读取日志
 * 筛选之后存入数据库
 **/
object HandleLog extends App {
  Logger.getLogger("org").setLevel(Level.ERROR) //配置日志

  val conf = new SparkConf().setMaster("local[*]").setAppName("logHandle")
  val ssc = new StreamingContext(conf, Seconds(2))

  val input = "./logs/" //日志存储路径
  val textStream = ssc.textFileStream(input)
  val con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "sys123")
  val wcStream = textStream.foreachRDD(_.map(line => {
    println("-------------parse---------------")
    var level = line.split("]")(0).replace("[", "") //截取每条日志的等级信息
    var date = line.split(":")(1) //截取日志的日期信息
    var info = line.split("\t")(1) //描述信息
    println("(" + level + "," + date + "," + info + ")")
    (level, info, date) //返回每条日志的信息
  })
    .filter(_._1.equals("error")) //筛选出日志等级为error的
    .foreach(x => {
      //保存至数据库
      val pstmt = con.prepareStatement("insert into logs(level,info,date) values(?,?,?)")
      pstmt.setString(1, x._1)
      pstmt.setString(2, x._2)
      pstmt.setString(3, x._3)
      pstmt.executeUpdate()
      pstmt.close()
      println("-------------Complete---------------")
      println()
    })
  )
  ssc.start()
  ssc.awaitTermination()
  con.close()
}