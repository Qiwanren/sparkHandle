package flowCount


import org.apache.spark.sql.{SparkSession, functions}

/***
  *
  * 测试练习自定义udf函数
  *
  */
object udfTest {

  //初始化spark
  val spark = SparkSession.builder()
    .appName("flowHandle")
    .master("local[2]")
    //.master("spark://192.168.0.110:7077")
    .getOrCreate()

  //注册udf函数
  val strLen = spark.udf.register("strLen", (str: String) => str.length())
  /**
    * 根据年龄大小返回是否成年 成年：true,未成年：false
    * 注册自定义函数（通过实名函数）
    */
  def isAdult(age: Int) = {
    if (age < 18) {
      false
    } else {
      true
    }

  }
  //UDF注册(普通方法注册成UDF)   Boolean 为返回值，Int为参数列表
  spark.udf.register("isAdult",isAdult(_:Int))

  def main(args: Array[String]): Unit = {
    println("测试练习udf········")
    test1()
  }

  def test1(): Unit ={
    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    //创建测试df
    val userDF = spark.createDataFrame(userData).toDF("name", "age")

    //引入隐式转换
    import spark.implicits._

    val separator = ","

    /***
      * 通过map操作将查询结果的列数据转换为行数据
      */
    //userDF.map(_.toSeq.foldLeft("")(_ + separator + _).substring(1)).show()
    //val userDF1 = userDF.map(_.toSeq.foldLeft("")(_ + separator + _).substring(1))
    //userDF1.show()

    //通过withColumn添加列
    //userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult",isAdult(col("age"))).show
    //通过select添加列
    //userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show

   //userDF.show
    // 注册一张user表
    userDF.createOrReplaceTempView("user")
    spark.sql("select name,strLen(name) as name_len,age,isAdult(age) as isAdult from user").show




  }
  //UDF注册(普通方法注册成UDF)
  //spark.udf.register("avgScoreUDF",functions.udf[Double,Int,Int,Int](avgScorePerStudent))
  /**UDF:一行输入一行输出。 这里,得到每个学生(每条数据)平均成绩。*/
//  def avgScorePerStudent(language:Int,math:Int,english:Int):Double={
//    ((language+math+english)/3.0).formatted("%.2f").toDouble
//  }



  case class Person(id:String,name:String,age:Int)

}
