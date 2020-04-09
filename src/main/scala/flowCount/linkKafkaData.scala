package flowCount

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf}

object linkKafkaData {

  def main(args: Array[String]): Unit = {
    print("begin to getKafkaData················")
    //构建conf ssc 对象
    val conf = new SparkConf().setAppName("Kafka_director").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //设置数据检查点进行累计统计单词
    //ssc.checkpoint("hdfs://qiwr1:9000/home/hadoop/data/checkpoint/")
    ssc.checkpoint("F:\\tmp\\checkpoint")
    //kafka 需要Zookeeper  需要消费者组
    val topics = Set("wordCount")
    //                                     broker的原信息                                  ip地址以及端口号
    val kafkaPrams = Map[String,String]("metadata.broker.list" -> "qiwr2:9092,qiwr3:9092")
    //                                          数据的输入了类型    数据的解码类型
    val data = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaPrams, topics)
    val updateFunc =(curVal:Seq[Int],preVal:Option[Int])=>{
      //进行数据统计当前值加上之前的值
      var total = curVal.sum
      //最初的值应该是0
      var previous = preVal.getOrElse(0)
      //Some 代表最终的但会值
      Some(total+previous)
    }
    //统计结果
    val result = data.map(_._2).flatMap(_.split(" ")).map(word=>(word,1)).updateStateByKey(updateFunc).print()
    //启动程序
    ssc.start()
    ssc.awaitTermination()

  }
}
