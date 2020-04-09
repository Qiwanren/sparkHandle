package flowCount

import org.apache.spark.{SparkConf, SparkContext}

object readLocalData {

  def main(args: Array[String]): Unit = {

    //创建配置，设置app的name
    //.setMaster("local[1]") 采用一个线程，在本地运行spark运行模式
    //.setMaster("spark://qiwr1:7077")  必须运行在集群上面
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[1]")
    //创建sparkContext，将conf传入
    val sc = new SparkContext(conf)
    //从文件中读取数据，进行计算，并将结果写入到文件系统中
    val rdd1 = sc.textFile("D:\\file\\spark\\input\\t_ext.txt")
    val rdd2 = rdd1.flatMap(_.split(" "))
    rdd2.collect().foreach{println}
    val rdd3 = rdd2.map(x=>(x,1)) //rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_+_)

    rdd4.collect().foreach{println}

    //rdd4.saveAsTextFile("F:\\spark\\output1")
    //停止
    sc.stop()
  }

}
