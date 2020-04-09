package utils

import java.util

import org.apache.spark.sql.SparkSession

object scalaUtils {
  //初始化spark
  val spark = SparkSession.builder()
    .appName("scalaUtils")
    .master("local[2]")
    //.master("spark://192.168.0.110:7077")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    // parallelize 的第二个参数指定了分区数
    val numberRdd = spark.sparkContext.parallelize(numberArray,1)
    // 每个元素乘以2
    //val nrdd = numberRdd.map(_ * 2)

    //过滤出里面的偶数元素
    //val nrdd1 = numberRdd.filter(_%2==0)
    //nrdd1.foreach(println(_))

    // flatMap - 构造集合
    //val lineArray = Array("hello you", "hello me", "hello world")
    //val lineRDD = spark.sparkContext.parallelize(lineArray,1)
    //lineRDD.foreach { word => println(word.split(" ")) }  //输出为三个元素
    //val lineRDD1 = lineRDD.flatMap(_.split(" "))    //输出为六个元素
    //lineRDD1.foreach(println(_))

    // groupByKey  根据key把数据分组，分组后的value形成一个新的列表
//    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),Tuple2("class1", 90), Tuple2("class2", 60))
//    val scores = spark.sparkContext.parallelize(scoreList, 1)
//    val groupedScores = scores.groupByKey()
//
//    groupedScores.foreach(score => {
//      println(score._1);
//      score._2.foreach { singleScore => println(singleScore) };
//      println("=============================")
//    })

    // groupByKey  根据key把数据分组，分组后的value形成一个新的列表
//    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),
//      Tuple2("class1", 90), Tuple2("class2", 60))
//    val scores = spark.sparkContext.parallelize(scoreList, 1)
//    val totalScores = scores.reduceByKey(_ + _)
//
//    totalScores.foreach(classScore => println(classScore._1 + ": " + classScore._2))

    // sortByKey  根据key对数据进行排序
//    val scoreList = Array(Tuple2(65, "leo"), Tuple2(50, "tom"),
//      Tuple2(100, "marry"), Tuple2(85, "jack"))
//    val scores = spark.sparkContext.parallelize(scoreList, 1)
//    val sortedScores = scores.sortByKey(false)
//    sortedScores.foreach(studentScore => println(studentScore._1 + ": " + studentScore._2))

    val studentList = Array(
      Tuple2(1, "leo"),
    Tuple2(2, "jack"),
    Tuple2(3, "tom"));

    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(3, 60));

    val sexList = Array(
      Tuple2(1, "男"),
      Tuple2(2, "女"),
      Tuple2(3, "男"));

    val students = spark.sparkContext.parallelize(studentList);
    val scores = spark.sparkContext.parallelize(scoreList);
    val sexs = spark.sparkContext.parallelize(sexList);

    val studentScores = students.join(scores)
    //val students = studentScores.join(sexs)

    studentScores.foreach(studentScore => {
      println("student id: " + studentScore._1);
      println("student name: " + studentScore._2._1)
      println("student socre: " + studentScore._2._2)
      println("=======================================")
    })

  }

}
