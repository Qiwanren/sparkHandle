package flowCount

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object flowCount {

  def readFile(filepath:String):String={
    return "hello "+filepath+" !"
  }

  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .config("spark.sql.warehouse.dir","/user/hive/warehouse").config("spark.shuffle.blockTransferService","nio")
//      .config("spark.files.ignoreCorruptFiles",value = true)
//      .config("spark.groupby.orderby.position.alias",value=true)
//      .enableHiveSupport()
//      .getOrCreate()
    val spark = SparkSession.builder()
        .appName("SparkSqlDemo")
        .master("local")
    //    .master("spark://192.168.0.110:7077")
        .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val peopleRDD = spark.sparkContext.textFile("D:\\file\\spark\\input\\people.txt")

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

    spark.sql("SELECT name,age FROM people").show()


    val results = spark.sql("SELECT name,age FROM people")
    //results.map(attributes => "Name: " + attributes(0)).show()

    //results.rdd.repartition(1).saveAsTextFile("D:\\file\\spark\\output\\people")
    results.toJavaRDD.saveAsTextFile("D:\\file\\spark\\output\\people")

  }

}
