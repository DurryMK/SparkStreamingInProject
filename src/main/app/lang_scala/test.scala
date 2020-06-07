package lang_scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志

    val conf = new SparkConf().setMaster("local[*]").setAppName("saveResultToText")

    val ssc = new StreamingContext(conf, Seconds(2))

    val input = "./logs/"

    val textStream = ssc.textFileStream(input)
    val wcStream = textStream.map(word => (word, 1)).reduceByKey(_ + _)
    wcStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
