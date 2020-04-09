package logAnalysis

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object flowData {

  def handleDate(appDate:String): String ={
    //截取字符串
    val dates = appDate.split(",")
    val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日")
    // 将字符串转换为时间值
    val logTime = dateFormat.parse(dates(0)).getTime
    // 格式化时间
    val format2 = new SimpleDateFormat("yyyy-MM-dd")
    return format2.format(logTime)
  }

  def main(args: Array[String]): Unit = {

    //初始化spark
    val spark = SparkSession.builder()
      .appName("flowHandle")
      .master("local[2]")
      //.master("spark://192.168.0.110:7077")
      .getOrCreate()

    //注册udf函数
    spark.udf.register("handleDate",handleDate(_:String))

    //读取数据
    //val flowDataRDD = spark.sparkContext.textFile("F:\\spark\\input\\GameLog.txt")
    val flowDataRDD = spark.sparkContext.textFile("D:\\file\\spark\\input\\GameLog.txt")
    val peosonRdd : RDD[GameLog]= flowDataRDD.map(_.split("\\|")).map(x => GameLog(x(0),x(1),x(2),x(3),x(4),x(5)))
    import spark.implicits._

    val peopleDF : DataFrame = peosonRdd.toDF()
    peopleDF.createOrReplaceTempView("t_login_log")

    val results = spark.sql("SELECT id,appDate,ip,userName,zhiye,sex FROM t_login_log t limit 10").show()
    val results1 = spark.sql("SELECT count(1) FROM t_login_log t").show()
    val results2 = spark.sql("SELECT count(1) FROM t_login_log t where handleDate(appDate) = '2016-02-01'").show()

    //将数据与字段进行关联
    //    val schemaString = "id name age"
    //    // Generate the schema based on the string of schema
    //    val fields = schemaString.split(" ")
    //      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    //    val schema = StructType(fields)
    //
    //    val rowRDD = flowDataRDD.map(_.split(","))
    //        .map(attributes => Row(attributes(0), attributes(1).trim,attributes(2).trim))
    //
    //    //将RDD对象转换为dataFrame
    //    val peopleDF = spark.createDataFrame(rowRDD, schema)
    //    peopleDF.createOrReplaceTempView("people")
    //    val results = spark.sql("SELECT id,name,age FROM people where age = '25'").show()

  }
  case class Person(id:String,name:String,age:Int)
  case class GameLog(id:String,appDate:String,ip:String,userName:String,zhiye:String,sex:String)

  //,field1:String,field2:String,field3:String
}