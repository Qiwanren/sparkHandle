package db

import org.apache.spark.sql.SparkSession

object JdbcLinkHiveDemo {

  case class t_ext(id: String, name: String,age:Int)

  def main(args: Array[String]): Unit = {
    LinkHiveByJdbc()
  }

  def LinkHiveByJdbc(): Unit ={

    val spark = SparkSession
      .builder().master("local")
      .appName("SparkHiveExample").config("hive.metastore.warehouse.dir", "hdfs://qiwr1:9000/user/hive/warehouse") //hive元数据的路径
      .config("hive.exec.scratchdir","E:/tmp")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use qwr")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (id STRING, name STRING,age INT) USING hive")
    //sql("LOAD DATA LOCAL INPATH '/tmp/hive/t_ext.txt' INTO TABLE src")
    //spark.sql("LOAD DATA INPATH 'hdfs://qiwr1:9000/hivedata/qwr/t_ext' INTO TABLE src")
    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM student").show()

  }



}
