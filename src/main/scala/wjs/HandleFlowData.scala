package wjs

import org.apache.spark.sql.SparkSession

object HandleFlowData {

  def getDate():String = {
    return "20190330";
  }

  def getData(path:String):String = {
    return path;
  }

  def main(args: Array[String]): Unit = {

    //初始化spark
    val spark = SparkSession.builder()
      .appName("flowHandle")
      .master("local[2]")
      //.master("spark://192.168.0.110:7077")
      .getOrCreate()

    //读取数据
    val flowData = spark.sparkContext.textFile("")

  }
}
