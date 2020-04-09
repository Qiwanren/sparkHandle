package utils

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CvsUtils {

  //初始化spark
  val spark = SparkSession.builder()
    .appName("flowHandle")
    .master("local[2]")
    //.master("spark://192.168.0.110:7077")
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    println(" 开始处理cvs数据·········· ")

    //一个StruceField你可以把它当成一个特征列。分别用列的名称和数据类型初始化
    val structFields = List(StructField("id",DoubleType),StructField("name",StringType),StructField("sex",StringType),StructField("content",StringType))
    //最后通过StructField的集合来初始化表的模式。
    val types = StructType(structFields)

    //1.定义了以一个HDFS文件（由数行文本组成）为基础的RDD
    //val lines = spark.sparkContext.textFile("D:\\data\\cvstest.csv")
    val path = "D:\\data\\cvstest.csv"
    val inputRdd = spark.sparkContext.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK"))
    val rowRdd = inputRdd.map(line=>Row(line.trim.split(",")(0).toDouble,line.trim.split(",")(1).toString,line.trim.split(",")(2).toString,line.trim.split(",")(3).toString))

    //通过SQLContext来创建DataFrame.
    val df = spark.sqlContext.createDataFrame(rowRdd,types)

    //df.show()

    df.createOrReplaceTempView("ts_user_info")

    spark.sql("select sex from ts_user_info").show()

    spark.stop()

  }



}
