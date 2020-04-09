package db

import java.io.File
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object JdbcLinkMySqlDemo {

  def writeDataToMySql(): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("SparkMysql")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local")
      .getOrCreate()
    val file = spark.sparkContext.textFile("hdfs://192.168.0.110:9000/spark/person.txt")
    //val file = spark.sparkContext.textFile(" F:\\spark\\input\\person.txt")
    val peosonRdd : RDD[People]= file.map(_.split(",")).map(x => People(x(0),x(1),x(2).toInt))
    import spark.implicits._
    val peosonDF : DataFrame = peosonRdd.toDF()
    peosonDF.createOrReplaceTempView("person")
    val p : Properties = new Properties()
    p.put("user","root")
    p.put("password","admin")
    spark.sql("select * from person").write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/workaide?useUnicode=true&characterEncoding=utf8","ts_user_info_tmp",p)
    spark.stop()
  }

  def linkMysqlByJdbc(): Unit ={
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("SparkMysql")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/workaide")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","ts_user_info")
      .option("user", "root")
      .option("password", "admin")
      .load()
    //jdbcDF.collect().foreach(println(_))
    //jdbcDF.createOrReplaceTempView("ts_user_info")

    val separator = ","
    val jdbcDF1 = jdbcDF.rdd.map(_.toSeq.foldLeft("")(_ + separator + _).substring(1))

    jdbcDF1.collect().foreach(println(_))
    jdbcDF1.saveAsTextFile("F:\\spark\\input\\t_user_info")

    //val resultText = spark.sql("select * from ts_user_info")
    //resultText.write.text("F:\\spark\\input\\t_user_info.txt")

  }

  def insertMysqlByJdbc(): Unit ={
    val spark = SparkSession.builder()
      .appName("SparkSqlDemo")
      .master("local")
      //    .master("spark://192.168.0.110:7077")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val peopleRDD = spark.sparkContext.textFile("D:\\file\\spark\\input\\person.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    //将RDD对象转换为dataFrame
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    //   直接获取DataFrame对象
    //    val peopleDF = spark.sparkContext
    //      .textFile("F:/spark/input/people.txt")
    //      .map(_.split(","))
    //      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    //      .toDF()
    peopleDF.createOrReplaceTempView("people")
    val p : Properties = new Properties()
    p.put("user","root")
    p.put("password","admin")
    //spark.sql("select * from people").write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/workaide?useUnicode=true&characterEncoding=utf8","people",p)
    spark.sql("select * from people").write.mode("append").jdbc("jdbc:mysql://localhost:3306/workaide?useUnicode=true&characterEncoding=utf8","people",p)
    spark.stop()

  }



  def main(args: Array[String]): Unit = {
    //linkMysqlByJdbc();
    //writeDataToMySql();
    insertMysqlByJdbc()
  }
  case class People(name:String,sex:String,age:Int)
}

