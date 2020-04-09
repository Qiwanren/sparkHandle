package wjs

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
//import unicomll.lsz.prok.MobData20.ystd
import org.slf4j.Logger
import org.slf4j.LoggerFactory
object MobData_30 {
   val logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  val ystd =getystd()
  val ystd2 =getystd2()
  val ysdt = getYestermo()
  val ysday = getYesterday()
  val ysday2 = getYesterday2()
  val ysday3 = getYesterday3()
  val McsSf = "hebei"
/**
  * 1.Input parameter list
*/
var HdfsIputSrc=s"hdfs://lisz1:9000/llsj_day/${ysday}/*"
val HdfsIputCell="hdfs://lisz1:9000/sqoop/mb1/*"//小区
val HdfsIputSector="hdfs://lisz1:9000/sqoop/mb2/*"//扇区
val HdfsIputStation="hdfs://lisz1:9000/sqoop/mb3/*"//基站
val HdfsIputWz="hdfs://lisz1:9000/sqoop/mb4/*"//无主

  //oracle para  /
  val user ="ubase"
  val password="Wjs3Gfzgh"
  val url="jdbc:oracle:thin:@10.162.65.120:5901:orcl"


//  val oracl_tb1=s"CELLDATA_${ysdt}"
//  val oracl_tb2=s"SECTIONDATA_${ysdt}"
//  val oracl_tb3=s"BASESTDATA_${ysdt}"
  val oracl_tb41=s"CELLDATA_${ysday}_DAY"
  val oracl_tb51=s"SECTIONDATA_${ysday}_DAY"
  val oracl_tb61=s"BASESTDATA_${ysday}_DAY"
  val oracl_tb42=s"CELLDATA_${ysday2}_DAY"
  val oracl_tb43=s"CELLDATA_${ysday3}_DAY"
  val oracl_tb52=s"SECTIONDATA_${ysday2}_DAY"
  val oracl_tb53=s"SECTIONDATA_${ysday3}_DAY"
  val oracl_tb62=s"BASESTDATA_${ysday2}_DAY"
  val oracl_tb63=s"BASESTDATA_${ysday3}_DAY"
  val oracl_tb7=s"NOCELLDATA_${ysday}"
  val oracl_tb8=s"NOCELLDATA${ysday}TMP3"


  def main(args: Array[String]): Unit = {
    val ystd =getystd()
    val ystd2 =getystd2()
    val ystd3 =getystd3()
    val ysdt = getYestermo()
    val ysday = getYesterday()
    val ysday2 = getYesterday2()
    HdfsIputSrc = args(0)
//    val McsSf = args(1)
    val nettype=39343

//    logger.info(s"the args ==>>>>>>>> hdfssrc : ${HdfsIputSrc} , oracl tb1 : ${oracl_tb1}  , ${oracl_tb8} ")
//    logger.info(s"the args ==>>>>>>>> dt : ${ysdt} : ${ystd}  , ${ysday} ")
//    logger.info(s"the args of db ===>>> oracltb ${oracl_tb1}  ${oracl_tb2}  ${oracl_tb3}  ${oracl_tb4}  ${oracl_tb5} ${oracl_tb6}  ${oracl_tb7}  ${oracl_tb8}  ")

/*
    if(args.length !=2){
      System.err.println("Useage :: <  >")
      System.exit(1)
    }*/

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir","/user/hive/warehouse").config("spark.shuffle.blockTransferService","nio")
      .config("spark.files.ignoreCorruptFiles",value = true)
      .config("spark.groupby.orderby.position.alias",value=true)
      .enableHiveSupport()
      .getOrCreate()
//    '1280','19202','08','77','75432','64736','140168','6971909','20180314'
    val firstRdd = spark.sparkContext.textFile(HdfsIputSrc)
    val mb1 = spark.sparkContext.textFile(HdfsIputCell)//小区
    val mb2 = spark.sparkContext.textFile(HdfsIputSector)//扇区
    val mb3 = spark.sparkContext.textFile(HdfsIputStation)//基站
    val mb4 = spark.sparkContext.textFile(HdfsIputWz)//无主
    spark.sparkContext.setCheckpointDir("hdfs://lisz1:9000/spark/checkpoint")

    val xqrdd = mb1.filter(z => z.startsWith("0")).map(_.split("\t")).filter(x => x.size >= 3 ).map(z =>
      if(z.size == 14){
      Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),z(7),z(8),z(9),z(10),z(11),z(12),z(13))
      }else if(z.size == 13){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),z(7),z(8),z(9),z(10),z(11),z(12),"")
      }else if(z.size == 12){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),z(7),z(8),z(9),z(10),z(11),"","")
      }else if(z.size == 11){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),z(7),z(8),z(9),z(10),"","","")
      }else if(z.size == 10){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),z(7),z(8),z(9),"","","","")
      }else if(z.size == 9){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),z(7),z(8),"","","","","")
      }else if(z.size == 8){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),z(7),"","","","","","")
      }else if(z.size == 7){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),"","","","","","","")
      }else if(z.size == 6){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),"","","","","","","","")
      }else if(z.size == 5){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),"","","","","","","","","")
      }else if(z.size == 4){
        Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),"","","","","","","","","","")
      }else{
        Row("","",-1.0,-1.0,"","","","","","","","","","")
      }
    )

 /*   val xqrdd = mb1.filter(z => z.startsWith("0")).map(_.split("\t")).
      map(z => Row(z(0),z(1).toString.trim,ismDouble(z(2)),ismDouble(z(3)),z(4),z(5),z(6),z(7),z(8),z(9),z(10),z(11),z(12),z(13)))*/

//cell src load then etl
/*    val scheamCell= "province_id,city_id,lac,cellid,net_type,node_type,station_id,section_id,cellinfo_id,cell_name,status,pro_stage,pro_name,pms_code"
    val fields = scheamCell.split(",").map(fieldName => StructField(fieldName,StringType,nullable = true))
    val scheamc1 = StructType(fields)*/
val scheamc1 = StructType(List(
  StructField("province_id", StringType),
  StructField("net_type", StringType),
  StructField("lac", DoubleType),
  StructField("cellid", DoubleType),
  StructField("city_id", StringType),
  StructField("node_type", StringType),
  StructField("station_id", StringType),
  StructField("section_id", StringType),
  StructField("cellinfo_id", StringType),
  StructField("cell_name", StringType),
  StructField("status", StringType),
  StructField("pro_stage", StringType),
  StructField("pro_name", StringType),
  StructField("pms_code", StringType)
))
    spark.createDataFrame(xqrdd,scheamc1).createOrReplaceTempView("xqtb1")

    spark.sql(
      """
        |select * from xqtb1 where lac != '-1' and cellid != '-1'
      """.stripMargin).createOrReplaceTempView("xqtb")

/*
        spark.sql(
      """select province_id,city_id,lac,cellid,net_type,node_type,station_id,section_id,cellinfo_id,
        |cell_name,status,pro_stage,pro_name,pms_code from xqtb1
        |group by province_id,city_id,lac,cellid,net_type,node_type,station_id,section_id,cellinfo_id,cell_name,
        |status,pro_stage,pro_name,pms_code
      """.stripMargin).createOrReplaceTempView("xqtb")
*/

/*    spark.sql(
      """select province_id,city_id,lac,cellid,net_type,node_type,station_id,section_id,cellinfo_id,
        |cell_name,status,pro_stage,pro_name,pms_code from xqtb1 where lac is not null and cellid is not null
        |group by province_id,city_id,lac,cellid,net_type,node_type,station_id,section_id,cellinfo_id,cell_name,
        |status,pro_stage,pro_name,pms_code
      """.stripMargin).createOrReplaceTempView("xqtb")*/


//    val xq = spark.createDataFrame(xqrdd,scheamc1).createOrReplaceTempView("xqtb")
    val xq2=spark.sql(
      s"""
        |select * from xqtb where net_type like '%01%'
      """.stripMargin)
    val xq3= spark.sql(
      s"""
        |select * from xqtb where net_type like '%02%'
      """.stripMargin)
    val xq4= spark.sql(
      s"""
        |select * from xqtb where net_type like '%03%' or net_type like '%04%'
      """.stripMargin)
//    xq4.select("*").show()
// sector src load then etl
val sqrdd = mb2.filter(z => !(z.startsWith("SLF4J:") || z.startsWith("18/")) ).map(_.split("\t")).filter(x => x.size >=3).map(z =>
  if(z.size == 6){Row(z(0),z(1),z(2),z(3),z(4),z(5))}else if(z.size == 5) {
    Row(z(0),z(1),z(2),z(3),z(4),"")
  }else if (z.size == 4){
    Row(z(0),z(1),z(2),z(3),"","")
  }else if (z.size == 3){
    Row(z(0),z(1),z(2),"","","")
  }else{
    Row("","","","","","")
  }
    )
/*        val scheamSector = "id,status,section_name,pro_stage,pro_name,pms_code"
        val scheamSq = scheamSector.split(",").map(fieldName => StructField(fieldName,StringType,nullable = true))
        val scheamc2 = StructType(scheamSq)*/
    val scheamc2 = StructType(List(
      StructField("id", StringType),
      StructField("status", StringType),
      StructField("section_name", StringType),
      StructField("pro_stage", StringType),
      StructField("pro_name", StringType),
      StructField("pms_code", StringType)
    ))
     spark.createDataFrame(sqrdd,scheamc2).createOrReplaceTempView("sqtb")
/*    spark.sql(
      """
        |select id,status,section_name,pro_stage,pro_name,pms_code from sqtb1 where id is not null
        |group by id,status,section_name,pro_stage,pro_name,pms_code
      """.stripMargin).createOrReplaceTempView("sqtb")*/

    //wzxq
    val wzrdd = mb4.filter(z => !(z.startsWith("SLF4J:") || z.startsWith("18/")) ).map(_.split("\t")).filter(x => x.size >=3).map(cc=>
      if(cc.size == 6) {
        Row(cc(0),cc(1),ismDouble(cc(2)),cc(3),cc(4),cc(5))
      }else if (cc.size == 5){
        Row(cc(0),cc(1),ismDouble(cc(2)),cc(3),cc(4),"")
      }else if (cc.size == 4){
        Row(cc(0),cc(1),ismDouble(cc(2)),cc(3),"","")
      }else if (cc.size == 3){
        Row(cc(0),cc(1),ismDouble(cc(2)),"","","")
      }else{
        Row("","",0.0,"","","")
      }

    )
/*        val scheamwzxq= "province_id,city_id,lac,type,remarks,if_reuse"
        val scheamWz=scheamwzxq.split(",").map(fieldName =>StructField(fieldName,StringType,nullable = true))
        val scheamc3= StructType(scheamWz)*/
    val scheamc3 = StructType(List(
      StructField("province_id", StringType),
      StructField("city_id", StringType),
      StructField("lac", DoubleType),
      StructField("type", StringType),
      StructField("remarks", StringType),
      StructField("if_reuse", StringType)
    ))
    spark.createDataFrame(wzrdd,scheamc3).createOrReplaceTempView("CELL_LAC_INUSE")
    
    //jz
    val jzrdd = mb3.filter(z => !(z.startsWith("SLF4J:") || z.startsWith("18/")) ).map(_.split("\t")).filter(z => z.size >=4).map(x =>
      if(x.size==9) {
        Row(x(0), x(1), x(2), x(3), x(4), ismDouble(x(5)), ismDouble(x(6)), ismDouble(x(7)), ismDouble(x(8)))
      }else if (x.size == 8){
        Row(x(0), x(1), x(2), x(3), x(4), ismDouble(x(5)), ismDouble(x(6)), ismDouble(x(7)), 0.0)
      }else if(x.size == 7){
        Row(x(0), x(1), x(2), x(3), x(4), ismDouble(x(5)), ismDouble(x(6)), 0.0, 0.0)
      }else if(x.size == 6){
      Row(x(0), x(1), x(2), x(3), x(4), ismDouble(x(5)), 0.0, 0.0, 0.0)
      }else if(x.size == 5){
        Row(x(0), x(1), x(2), x(3), x(4),0.0, 0.0, 0.0, 0.0)
      }else if(x.size == 4){
        Row(x(0), x(1), x(2), x(3),"",0.0, 0.0, 0.0, 0.0)
      }else{
      Row("","","","","", 0.0,0.0,0.0, 0.0)
    }

    )
/*        val scheamJi = "id,status,station_name,node_code,node_type,longitude_plan,latitude_plan,longitude_build,latitude_build"
        val scheamJZ =scheamJi.split(",").map(fieldName => StructField(fieldName,StringType,nullable = true))
        val scheamj1 = StructType(scheamJZ)*/
    val scheamj1 = StructType(List(
      StructField("id", StringType),
      StructField("status", StringType),
      StructField("station_name", StringType),
      StructField("node_code", StringType),
      StructField("node_type", StringType),
      StructField("longitude_plan", DoubleType),
      StructField("latitude_plan", DoubleType),
      StructField("longitude_build", DoubleType),
      StructField("latitude_build", DoubleType)
    ))
    spark.createDataFrame(jzrdd,scheamj1).createOrReplaceTempView("jztb")
/*    spark.sql(
      """
        |select id,status,station_name,node_code,node_type,longitude_plan,latitude_plan,longitude_build,latitude_build
        |from jztb1 where id is not null group by id,status,station_name,
        |node_code,node_type,longitude_plan,latitude_plan,longitude_build,latitude_build
      """.stripMargin).createOrReplaceTempView("jztb")*/
    
// lac   cellid hour count updytes downbytes sumbytes duration cell_date
val scheam = StructType(List(
  StructField("province_id", StringType, nullable = false),
  StructField("lac", DoubleType, nullable = false),
  StructField("cellid", DoubleType, nullable = false),
  StructField("hour", StringType, nullable = false),
  StructField("count", DoubleType, nullable = false),
  StructField("duration", DoubleType, nullable = false),
  StructField("upbytes", DoubleType, nullable = false),
  StructField("downbytes", DoubleType, nullable = false),
  StructField("sumbytes", DoubleType, nullable = false),
  StructField("cell_date", StringType, nullable = false)
))

        val srcRdd =firstRdd.map(_.split(","))
      .filter(x =>x.size==10).filter(y => kkfilter(y))
      .map(y => Row("0"+y(0),ismDouble(y(1)),ismDouble(y(2)),y(3)
        ,ismDouble(y(4)),ismDouble(y(5))
        ,ismDouble(y(6)),ismDouble(y(7))
        ,ismDouble(y(8)),y(9).trim))

/*    val srcRdd =firstRdd.map(_.split(","))
      .filter(x =>x.size==10)
      .map(y => Row("0"+y(0),ismInt(y(1)),ismInt(y(2)),y(3)
        ,ismDouble(y(4)),ismDouble(y(5))
        ,ismDouble(y(6)),ismDouble(y(7))
        ,ismDouble(y(8)),y(9).trim))*/

//    srcRdd.saveAsTextFile("hdfs://lisz1:9000/cs/20180715/srcRdd_jg")
/*    val srcRdd =firstRdd.map(_.split(","))
      .filter(x =>inspect(x))
      .map(y => Row("0"+(y(0).replace("'","")),y(1).replace("'","").toInt,y(2).replace("'","").toInt,y(3).replace("'","")
        ,y(4).replace("'","").toDouble,y(5).replace("'","").toDouble
        ,y(6).replace("'","").toDouble,y(7).replace("'","").toDouble
        ,y(8).replace("'","").toDouble,y(9).replace("'","").trim))*/


    val lszz= spark.createDataFrame(srcRdd,scheam)
    lszz.createOrReplaceTempView("dt1")
//province_id , substr(lac,1,10),substr(cellid,1,11),cell_date
//    val data1 = spark.sql(

   val data1= spark.sql(
      s"""
        |select
        |province_id,
        |substr(lac,1,10) lac,
        |substr(cellid,1,11) cellid,
        |cell_date,
        |hour,
        |sum(upbytes) upbytes,
        |sum(downbytes) downbytes,
        |sum(sumbytes) sumbytes,
        |sum(COUNT) count,
        |SUM(duration) duration
        |from dt1 where lac is not null and cellid is not null
        |group by
        |province_id,lac,cellid,cell_date,hour
      """.stripMargin)
//    data1.show()
    val data_p4g_tmp = data1.select("*").where("lac <0")
//    data_p4g_tmp.write.csv("hdfs://lisz1:9000/cs/20180715/data_p4g_tmp")
//      data_p4g_tmp.show()
      data_p4g_tmp.createOrReplaceTempView("tp_ls4g")
    val data_p4g = spark.sql(
      """
        |select province_id,regexp_replace(cast(lac as String),"-","")  lac,
        |cellid,cell_date,hour,upbytes,downbytes,sumbytes,count,duration
        |from tp_ls4g
      """.stripMargin)
//    data_p4g.show()
    val data_p2g = data1.select("*").where(s"lac <$nettype and lac >=0")
    val data_p3g = data1.select("*").where(s"lac >=$nettype")

    data_p4g.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/data_p4g")
    data_p3g.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/data_p3g")
    data_p2g.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/data_p2g")
    xq3.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/xq3")
    xq2.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/xq2")
    xq4.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/xq4")

    //join cell_tb

    data_p4g.createOrReplaceTempView("lsz_4g_07")
    xq4.createOrReplaceTempView("hh_4g_07")
    val cell_4g_1 =spark.sql(
      """
        |select t2.province_id province_id,t2.city_id city_id,t1.lac lac,
        |t1.cellid cellid,t2.net_type net_type,t2.node_type node_type,t1.cell_date cell_date,
        |t1.hour hour,t1.upbytes upbytes,t1.downbytes downbytes,t1.sumbytes sumbytes,t1.count count,t1.duration duration,t2.station_id  station_id,
        |t2.section_id section_id,t2.cellinfo_id cellinfo_id,t2.cell_name cell_name,
        |t2.status status,t2.pro_stage pro_stage,t2.pro_name pro_name,t2.pms_code pms_code  from lsz_4g_07 t1 left join hh_4g_07 t2
        |on t1.lac = t2.lac and t1.cellid = t2.cellid
      """.stripMargin).createOrReplaceTempView("cell_tm1")
/*    val cell_4g =data_p4g.as("t1").join(xq4.as("t2"),data_p4g.col("lac") === xq4.col("lac") && data_p4g.col("cellid") === xq4.col("cellid"),"left").
      select("t2.province_id","t2.city_id","t1.lac",
        "t1.cellid","t2.net_type","t2.node_type","t1.cell_date",
      "t1.hour","t1.upbytes","t1.downbytes","t1.sumbytes",
        "t1.count","t1.duration","t2.station_id",
        "t2.section_id","t2.cellinfo_id","t2.cell_name",
        "t2.status","t2.pro_stage","t2.pro_name",
        "t2.pms_code").createOrReplaceTempView("cell_tm1")*/

//  spark.sql("""select * from cell_tm1""").show()
    data_p3g.createOrReplaceTempView("lsz_3g_07")
    xq3.createOrReplaceTempView("hh_3g_07")
    val cell_3g = spark.sql(
      """
        |select t2.province_id  province_id,t2.city_id city_id,t1.lac lac,
        |t1.cellid cellid,t2.net_type net_type,t2.node_type node_type,t1.cell_date cell_date,
        |t1.hour hour,t1.upbytes upbytes,t1.downbytes downbytes,t1.sumbytes sumbytes,
        |t1.count count,t1.duration duration,t2.station_id station_id,
        |t2.section_id section_id,t2.cellinfo_id cellinfo_id,t2.cell_name cell_name,
        |t2.status status,t2.pro_stage pro_stage,t2.pro_name pro_name,
        |t2.pms_code  pms_code from lsz_3g_07 t1 left join hh_3g_07 t2 on
        |t1.lac = t2.lac and t1.cellid = t2.cellid
      """.stripMargin).createOrReplaceTempView("cell_tm2")
/*    val cell_3g =data_p3g.as("t1").join(xq3.as("t2"),data_p3g.col("lac") === xq3.col("lac") && data_p3g.col("cellid") === xq3.col("cellid"),"left").
      select("t2.province_id","t2.city_id","t1.lac",
        "t1.cellid","t2.net_type","t2.node_type","t1.cell_date",
        "t1.hour","t1.upbytes","t1.downbytes","t1.sumbytes",
        "t1.count","t1.duration","t2.station_id",
        "t2.section_id","t2.cellinfo_id","t2.cell_name",
        "t2.status","t2.pro_stage","t2.pro_name",
        "t2.pms_code").createOrReplaceTempView("cell_tm2")*/
//    spark.sql("""select * from cell_tm2""").show()
//    xq2.show()
    data_p2g.createOrReplaceTempView("lsz_2g_07")
    xq2.createOrReplaceTempView("hh_2g_07")
    val cell_2g= spark.sql("""select  t2.province_id province_id,t2.city_id city_id,t1.lac lac,
                               |t1.cellid cellid,t2.net_type net_type,t2.node_type node_type,t1.cell_date cell_date,
                               |t1.hour hour,t1.upbytes upbytes,t1.downbytes downbytes,t1.sumbytes  sumbytes,
                               |t1.count count,t1.duration duration,t2.station_id station_id,
                               |t2.section_id section_id,t2.cellinfo_id cellinfo_id,t2.cell_name cell_name,
                               |t2.status status,t2.pro_stage pro_stage,t2.pro_name pro_name,
                               |t2.pms_code pms_code from lsz_2g_07 t1 left join  hh_2g_07 t2  on (t1.province_id = t2.province_id and t1.lac = t2.lac and t1.cellid = t2.cellid)""".stripMargin).createOrReplaceTempView("cell_tm3")
//    val cell_2g =data_p2g.as("t1").join(xq2.as("t2"),data_p2g.col("lac") === xq2.col("lac") && data_p2g.col("cellid") === xq2.col("cellid"),"left").
//      select("t1.province_id","t2.city_id","t1.lac",
//        "t1.cellid","t2.net_type","t2.node_type","t1.cell_date",
//        "t1.hour","t1.upbytes","t1.downbytes","t1.sumbytes",
//        "t1.count","t1.duration","t2.station_id",
//        "t2.section_id","t2.cellinfo_id","t2.cell_name",
//        "t2.status","t2.pro_stage","t2.pro_name",
//        "t2.pms_code").createOrReplaceTempView("cell_tm3")
//    spark.sql("""select * from cell_tm1""").show()

    val aa = spark.sql("""select * from cell_tm1""")
    val bb = spark.sql("""select * from cell_tm2""")
    val cc = spark.sql("""select * from cell_tm3""")

    aa.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/cell_tm1")
    bb.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/cell_tm2")
    cc.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/cell_tm3")

    val cell_f1 = spark.sql(
      s"""
        |select
        |province_id ,
        |city_id,
        |lac ,
        |cellid ,
        |net_type,
        |node_type,
        |cell_DATE ,
        |hour,
        |station_id ,
        |section_id ,
        |cellinfo_id ,
        |cell_name ,
        |status,
        |pro_stage,
        |pro_name ,
        |pms_code,
        |sum(upbytes) upbytes,
        |sum(downbytes) downbytes,
        |sum(sumbytes) sumbytes,
        |sum(count) count,
        |SUM(duration) duration
        |from cell_tm1 where province_id is not null and net_type is not null
        |group by
        |province_id ,
        |city_id,
        |lac ,
        |cellid ,
        |net_type,
        |node_type,
        |cell_DATE ,
        |hour,
        |station_id ,
        |section_id ,
        |cellinfo_id ,
        |cell_name ,
        |status,
        |pro_stage,
        |pro_name ,
        |pms_code
      """.stripMargin)


//    cell_f1.checkpoint()
    val cell_f2 = spark.sql(
      s"""
         |select
         |province_id,
         |city_id,
         |lac ,
         |cellid ,
         |net_type,
         |node_type,
         |cell_DATE ,
         |hour,
         |station_id ,
         |section_id ,
         |cellinfo_id ,
         |cell_name ,
         |status,
         |pro_stage,
         |pro_name ,
         |pms_code,
         |sum(upbytes) upbytes,
         |sum(downbytes) downbytes,
         |sum(sumbytes) sumbytes,
         |sum(count) count,
         |SUM(duration) duration
         |from cell_tm2 where  province_id != '' and  net_type != ''
         |group by
         |province_id,
         |city_id,
         |lac ,
         |cellid ,
         |net_type,
         |node_type,
         |cell_DATE ,
         |hour,
         |station_id ,
         |section_id ,
         |cellinfo_id ,
         |cell_name ,
         |status,
         |pro_stage,
         |pro_name ,
         |pms_code
      """.stripMargin)

//    cell_f2.checkpoint()
    val cell_f3 = spark.sql(
      s"""
         |select
         |province_id ,
         |city_id,
         |lac ,
         |cellid ,
         |net_type,
         |node_type,
         |cell_DATE ,
         |hour,
         |station_id ,
         |section_id ,
         |cellinfo_id ,
         |cell_name ,
         |status,
         |pro_stage,
         |pro_name ,
         |pms_code,
         |sum(upbytes) upbytes,
         |sum(downbytes) downbytes,
         |sum(sumbytes) sumbytes,
         |sum(count) count,
         |SUM(duration) duration
         |from cell_tm3 where province_id  != ''  and net_type  != ''
         |group by
         |province_id ,
         |city_id,
         |lac ,
         |cellid ,
         |net_type,
         |node_type,
         |cell_DATE ,
         |hour,
         |station_id ,
         |section_id ,
         |cellinfo_id ,
         |cell_name ,
         |status,
         |pro_stage,
         |pro_name ,
         |pms_code
      """.stripMargin)
//    cell_f3.cache()
//    cell_f3.checkpoint()

      val first_jg = (cell_f1.union(cell_f2).union(cell_f3))
//    first_jg.show(20)
      first_jg.checkpoint()

//    first_jg.show()
    //read sqtb todataSet
//    #####################################################


        first_jg.createOrReplaceTempView("tmpsq")
    val sq_gl = spark.sql(
      s"""
        |select
        |t.province_id t_province_id,
        |t.city_id t_city_id,
        |t.LAC t_lac,
        |t.CELL_DATE t_cell_date,
        |t.NET_TYPE t_net_type,
        |t.node_type t_node_type,
        |t.HOUR t_cell_hour,
        |t.upbytes upbytes,
        |t.downbytes downbytes,
        |t.sumbytes sumbytes,
        |t.count count,
        |t.duration duration,
        |t.station_id  t_station_id,
        |t.section_id  t_section_id,
        |b.STATUS b_status,
        |b.SECTION_NAME b_section_name,
        |b.PRO_STAGE b_pro_stage,
        |b.PRO_NAME b_pro_name,
        |b.PMS_CODE b_pms_code
        |from tmpsq t, sqtb b
        |where t.section_id = b.id
      """.stripMargin)
    logger.info("===================>  4i'm here     <=============")
      sq_gl.createOrReplaceTempView("sqls")
//    spark.sql("select * from sqls").show()
/*
    province_id,
    city_id,
    lac,
    cell_date,
    net_type,
    node_type,
    cell_hour,
    upbytes,
    downbytes,
    sumbytes,
    cell_count,
    duration,
    station_id,
    section_id,
    status,
    section_name,
    pro_stage,
    pro_name,
    pms_code,
    max_available_speed,
    max_video_speed,
    max_file_speed,
    max_webbrowsing_speed
*/
      val second_jg = (spark.sql(
        """select
          |t_province_id province_id ,
          |t_city_id city_id,
          |t_lac  lac,
          |t_cell_date cell_date,
          |t_net_type net_type,
          |t_node_type node_type,
          |t_cell_hour cell_hour,
          |SUM(upbytes) upbytes,
          |SUM(downbytes) downbytes,
          |SUM(sumbytes) sumbytes,
          |sum(count) count,
          |sum(duration) duration,
          |t_station_id station_id,
          |t_section_id section_id,
          |b_status status,
          |b_section_name section_name,
          |b_pro_stage pro_stage,
          |b_pro_name pro_name,
          |b_pms_code pms_code
          |from sqls group by
          |t_province_id,
          |t_city_id,
          |t_lac,
          |t_cell_date,
          |t_net_type,
          |t_node_type,
          |t_cell_hour,
          |t_station_id,
          |t_section_id,
          |b_status,
          |b_section_name,
          |b_pro_stage,
          |b_pro_name,
          |b_pms_code
        """.stripMargin))
    second_jg.checkpoint()
    logger.info("===================>  1i'm here     <=============")
//second_jg.show()
    //jz guanlian
    second_jg.createOrReplaceTempView("jztmp1")
    logger.info("===================>  2i'm here     <=============")

    val jzgl =spark.sql(
      """
        |select
        |t.province_id province_id,
        |t.city_id city_id,
        |t.LAC lac,
        |t.CELL_DATE cell_date,
        |t.CELL_HOUR cell_hour,
        |t.NET_TYPE net_type,
        |t.upbytes upbytes,
        |t.downbytes downbytes,
        |t.sumbytes sumbytes,
        |t.count  count,
        |t.duration duration,
        |t.STATION_ID station_id,
        |b.STATUS status,
        |b.STATION_NAME station_name,
        |b.NODE_CODE node_code,
        |b.NODE_TYPE node_type,
        |b.LONGITUDE_PLAN longitude_plan,
        |b.LATITUDE_PLAN latitude_plan,
        |b.LONGITUDE_BUILD longitude_build,
        |b.LATITUDE_BUILD latitude_build,
        |t.PRO_STAGE pro_stage,
        |t.PRO_NAME pro_name,
        |t.PMS_CODE pms_code
        |from jztmp1 t , jztb b
        |where t.station_id = b.id
      """.stripMargin)
    jzgl.createOrReplaceTempView("zjjg")
    val third_jg =(spark.sql(
      """
        |select
        |province_id,
        |city_id,
        |LAC,
        |CELL_DATE,
        |CELL_HOUR,
        |NET_TYPE,
        |SUM(upbytes) upbytes,
        |SUM(downbytes) downbytes,
        |SUM(sumbytes) sumbytes,
        |sum(count)  count,
        |SUM(duration) duration,
        |STATION_ID,
        |STATUS,
        |STATION_NAME,
        |NODE_CODE,
        |NODE_TYPE,
        |LONGITUDE_PLAN,
        |LATITUDE_PLAN,
        |LONGITUDE_BUILD,
        |LATITUDE_BUILD,
        |PRO_STAGE,
        |PRO_NAME,
        |PMS_CODE
        |from zjjg
        |group by
        |province_id,
        |city_id,
        |LAC,
        |CELL_DATE,
        |NET_TYPE,
        |CELL_HOUR,
        |STATION_ID,
        |STATUS,
        |STATION_NAME,
        |LONGITUDE_PLAN,
        |LATITUDE_PLAN,
        |LONGITUDE_BUILD,
        |NODE_CODE,
        |NODE_TYPE,
        |LATITUDE_BUILD,
        |PRO_STAGE,
        |PRO_NAME,
        |PMS_CODE
      """.stripMargin))
    third_jg.checkpoint()

    first_jg.createOrReplaceTempView("tmp_jg4")
    val Fourth_jg=(spark.sql(
      """
        |select
        |province_id,
        |city_id ,
        |lac ,
        |cellid ,
        |net_type ,
        |node_type,
        |cell_date ,
        |SUM(upbytes) upbytes ,
        |SUM(downbytes) downbytes,
        |SUM(sumbytes) sumbytes ,
        |SUM(count) count,
        |SUM(duration) duration ,
        |station_id ,
        |section_id,
        |cellinfo_id ,
        |cell_name ,
        |status,
        |pro_stage ,
        |pro_name,
        |pms_code
        |from tmp_jg4
        |group by
        |province_id,
        |city_id ,
        |lac ,
        |cellid ,
        |net_type ,
        |node_type,
        |cell_date ,
        |station_id ,
        |section_id,
        |cellinfo_id ,
        |cell_name ,
        |status,
        |pro_stage ,
        |pro_name,
        |pms_code
      """.stripMargin))
    Fourth_jg.checkpoint()
    Fourth_jg.createOrReplaceTempView("lsz4jg")
    val Fourth_jg1 = spark.sql(s"""select * from lsz4jg where cell_date = '${ystd}' """)
    Fourth_jg1.checkpoint()
    val Fourth_jg2 = spark.sql(
      s"""
         |select * from lsz4jg where cell_date = '${ystd2}'
       """.stripMargin)
    Fourth_jg2.checkpoint()
    val Fourth_jg3 = spark.sql(
      s"""
         |select * from lsz4jg where cell_date = '${ystd3}'
       """.stripMargin)
    Fourth_jg3.checkpoint()

    second_jg.createOrReplaceTempView("tmp_jg5")

    val Fifth_jg =(spark.sql(
      """
        |select
        |province_id,
        |city_id ,
        |lac ,
        |cell_date ,
        |net_type ,
        |node_type,
        |SUM(upbytes) upbytes ,
        |SUM(downbytes) downbytes,
        |SUM(sumbytes) sumbytes ,
        |SUM(count) count,
        |SUM(duration) duration ,
        |pro_name,
        |pro_stage ,
        |section_name,
        |section_id,
        |station_id ,
        |status,
        |pms_code
        |from  tmp_jg5 group by
        |province_id,
        |city_id,
        |LAC,
        |CELL_DATE,
        |NET_TYPE,
        |node_type,
        |STATION_ID,
        |SECTION_ID,
        |STATUS,
        |SECTION_NAME,
        |PRO_STAGE,
        |PRO_NAME,
        |PMS_CODE
      """.stripMargin))
    Fifth_jg.checkpoint()
    Fifth_jg.createOrReplaceTempView("lsz5jg")
    val Fifth_jg1 = spark.sql(s"""select * from lsz5jg where cell_date = '${ystd}' """)
    Fifth_jg1.checkpoint()
    val Fifth_jg2 = spark.sql(
      s"""
         |select * from lsz5jg where cell_date = '${ystd2}'
       """.stripMargin)
    Fifth_jg2.checkpoint()
    val Fifth_jg3 = spark.sql(
      s"""
         |select * from lsz5jg where cell_date = '${ystd3}'
       """.stripMargin)
    Fifth_jg3.checkpoint()

    logger.info("===================>  GAO SHEN ME GUI     <=============")

    third_jg.createOrReplaceTempView("tmp_jg6")

    val Sixth_jg =(spark.sql(
      """
        |select
        |province_id,
        |city_id,
        |lac,
        |CELL_DATE,
        |SUM(upbytes) upbytes ,
        |SUM(downbytes) downbytes,
        |SUM(sumbytes) sumbytes ,
        |SUM(count) count,
        |SUM(duration) duration ,
        |STATION_NAME ,
        |NODE_CODE,
        |NET_TYPE ,
        |NODE_TYPE,
        |LONGITUDE_PLAN ,
        |LATITUDE_PLAN,
        |LONGITUDE_BUILD,
        |LATITUDE_BUILD ,
        |PRO_STAGE,
        |PRO_NAME,
        |PMS_CODE,
        |STATUS,
        |STATION_ID
        |from  tmp_jg6 group by
        |province_id,
        |city_id,
        |LAC,
        |CELL_DATE,
        |STATION_NAME,
        |NODE_CODE,
        |NET_TYPE ,
        |NODE_TYPE,
        |LONGITUDE_PLAN ,
        |LATITUDE_PLAN  ,
        |LONGITUDE_BUILD,
        |LATITUDE_BUILD ,
        |PRO_STAGE,
        |PRO_NAME,
        |PMS_CODE,
        |STATUS,
        |STATION_ID
      """.stripMargin)).cache()
    Sixth_jg.checkpoint()

    Sixth_jg.createOrReplaceTempView("lsz6jg")
    val Sixth_jg1 = spark.sql(s"""select * from lsz6jg where cell_date = '${ystd}' """)
    Sixth_jg1.checkpoint()
    val Sixth_jg2 = spark.sql(
      s"""
         |select * from lsz6jg where cell_date = '${ystd2}'
       """.stripMargin)
    Sixth_jg2.checkpoint()
    val Sixth_jg3 = spark.sql(
      s"""
         |select * from lsz6jg where cell_date = '${ystd3}'
       """.stripMargin)

    logger.info("===================>  3i'm here     <=============")

    // No main district
    //2,3,4G
//     cell_f1.createOrReplaceTempView("4g_ys")
//     cell_f2.createOrReplaceTempView("3g_ys")
//     cell_f2.createOrReplaceTempView("2g_ys")
    val wz_4g = spark.sql(
      """
        |select lac,CONCAT("-",lac) lac_,cellid,sumbytes," "  province_id from cell_tm1
        |where cellid != 65535 and province_id is null
      """.stripMargin)

    val wz_3g = spark.sql(
      """
        |select lac,lac lac_,cellid,sumbytes," " province_id  from cell_tm2
        |where cellid != 65535 and province_id is null
      """.stripMargin)

     val wz_2g=spark.sql(
      """
        |select lac,lac lac_,cellid,sumbytes,province_id
        |from cell_tm3
        |where city_id is null or city_id = ''
      """.stripMargin)
    val Seventh_jg=(wz_2g.union(wz_3g).union(wz_4g)).cache()
    Seventh_jg.checkpoint()

      //dx  01
    //不需要操作 TEMP_DX_4G
    data_p3g.select("lac")


    //02 lt
    wz_4g.createOrReplaceTempView("etlt4g")
    spark.sql(
      """
        |select distinct lac,cellid,sumbytes from etlt4g where  lac is not null and cellid is not null
      """.stripMargin).createOrReplaceTempView("temp_lt_4g")

    //03 23g
    wz_3g.createOrReplaceTempView("etlt3g")
    wz_2g.createOrReplaceTempView("etlt2g")
      spark.sql(
        """
          |select distinct lac,cellid from etlt3g  where  lac is not null and cellid is not null
          |union
          |select distinct lac,cellid from etlt2g where lac is not null and cellid is not null
        """.stripMargin).createOrReplaceTempView("temp_lt_23g")


    // 04 TEMP_LAC_INUSE_DX_4G

    spark.sql(
      """
        |select distinct province_id,city_id,lac,type from CELL_LAC_INUSE where type = '10'
      """.stripMargin).createOrReplaceTempView("temp_lac_inuse_dx_4g")
    //05 TEMP_LAC_INUSE_LT_4G_ZY
     spark.sql(
      """
        |SELECT DISTINCT PROVINCE_ID, IF_REUSE, CITY_ID, LAC, TYPE
        |					  FROM CELL_LAC_INUSE T1
        |					 WHERE EXISTS (SELECT 1
        |				FROM (SELECT LAC, COUNT(DISTINCT IF_REUSE) CNT
        |									  FROM CELL_LAC_INUSE
        |									 WHERE TYPE = '03'
        |										OR TYPE = '04'
        |									 GROUP BY LAC) T2
        |							 WHERE T2.CNT = 1
        |							   AND T1.LAC = T2.LAC)
        |					   AND (T1.TYPE = '03' OR T1.TYPE = '04')
      """.stripMargin)createOrReplaceTempView("temp_lac_inuse_lt_4g_zy")
    //06

    spark.sql(
      """
        |SELECT distinct PROVINCE_ID, IF_REUSE, CITY_ID, LAC, TYPE
        |					  FROM CELL_LAC_INUSE T1
        |					 WHERE EXISTS (SELECT 1
        |						FROM (SELECT LAC, COUNT(DISTINCT IF_REUSE) CNT
        |									  FROM CELL_LAC_INUSE
        |									 WHERE TYPE = '03'
        |										OR TYPE = '04'
        |									 GROUP BY LAC) T2
        |							 WHERE T2.CNT = 2
        |							   AND T1.LAC = T2.LAC) AND (T1.TYPE = '03' OR T1.TYPE = '04')
      """.stripMargin).createOrReplaceTempView("temp_lac_inuse_lt_4g_fy")

    //07
   spark.sql(
      """
        |select distinct PROVINCE_ID,
        |CITY_ID,
        |LAC,
        |TYPE
        |from CELL_LAC_INUSE where type = '01' or type = '02'
      """.stripMargin).createOrReplaceTempView("temp_lac_inuse_lt_23g")

    //a

//    TEMP_LT_4G  ND 数据表和TEMP_LAC_INUSE_LT_4G_ZY
/*    1、 ND.CELLID < 128
    2、 AND CL.IF_REUSE = 0
    3、 AND CL.PROVINCE_ID NOT IN ('011', '090')
    最后对获取的结果集进行去重操作，获取的字段如下
    1、 NVL(CL.PROVINCE_ID, '0') PROVINCE_ID,
    2、 NVL(CL.CITY_ID, '0') CITY_ID,
    3、 ND.LAC,
    4、 ND.LAC_,
    5、 ND.CELLID,
    6、 NVL(TYPE, '05') NET_TYPE,
    7、 1 STATUS*/
    val jg_a =spark.sql(
      """
        |SELECT DISTINCT NVL(CL.PROVINCE_ID, '0') PROVINCE_ID,
        |				NVL(CL.CITY_ID, '0') CITY_ID,
        |				ND.LAC,
        |				ND.CELLID,
        |				NVL(TYPE, '05') NET_TYPE,
        |				1 STATUS
        |  FROM TEMP_LT_4G ND
        |  LEFT JOIN TEMP_LAC_INUSE_LT_4G_ZY CL ON ND.LAC = CL.LAC
        | WHERE ND.CELLID < 128
        |   AND CL.IF_REUSE = 0
        |   AND CL.PROVINCE_ID NOT IN ('011', '090')
      """.stripMargin)
    //b

/*    b． 从TEMP_LT_4G  ND 数据表和TEMP_LAC_INUSE_LT_4G_ZY   CL数据表通过lac进行左关联，筛选出符合以下规则的数据
    1、 CL.PROVINCE_ID IN ('' 011 '', '' 090 '')
    2、 CL.IF_REUSE = 0
    最后对获取的结果集进行去重操作，获取的字段如下
    1、 NVL(CL.PROVINCE_ID, '0') PROVINCE_ID,
    2、 NVL(CL.CITY_ID, '0') CITY_ID,
    3、 ND.LAC,
    4、 ND.LAC_,
    5、 ND.CELLID,
    6、 NVL(TYPE, '05') NET_TYPE,*/
    val jg_b= spark.sql(
      """
        |SELECT DISTINCT NVL(CL.PROVINCE_ID, '0') PROVINCE_ID,
        |				NVL(CL.CITY_ID, '0') CITY_ID,
        |				ND.LAC,
        |				ND.CELLID,
        |				NVL(TYPE, '05') NET_TYPE,
        |				1 STATUS
        |  FROM TEMP_LT_4G ND
        |  LEFT JOIN TEMP_LAC_INUSE_LT_4G_ZY CL ON ND.LAC = CL.LAC
        | WHERE CL.PROVINCE_ID IN ('011', '090')
        |   AND CL.IF_REUSE = 0
      """.stripMargin)


    //c

/*    c． 从TEMP_LT_4G  ND 数据表和TEMP_LAC_INUSE_LT_4G_ZY   CL数据表通过lac进行左关联，筛选出符合以下规则的数据
    1、 ND.CELLID >= 128
    2、 CL.IF_REUSE = 1
    最后对获取的结果集进行去重操作，获取的字段如下
    1、 NVL(CL.PROVINCE_ID, '' 0 '') PROVINCE_ID,
    2、 NVL(CL.CITY_ID, '' 0 '') CITY_ID,
    3、 ND.LAC,
    4、 ND.LAC_,
    5、 ND.CELLID,
    6、 NVL(TYPE, '' 05 '') NET_TYPE,
    7、 1 STATUS*/

    val jg_c =spark.sql(
      """
        |SELECT DISTINCT NVL(CL.PROVINCE_ID, '0') PROVINCE_ID,
        |				NVL(CL.CITY_ID, '0') CITY_ID,
        |				ND.LAC,
        |				ND.CELLID,
        |				NVL(TYPE, '05') NET_TYPE,
        |				1 STATUS
        |  FROM TEMP_LT_4G ND
        |  LEFT JOIN TEMP_LAC_INUSE_LT_4G_ZY CL ON ND.LAC = CL.LAC
        | WHERE ND.CELLID >= 128
        |   AND CL.IF_REUSE = 1
      """.stripMargin)

    //d
/*  判断调整为spark sql
    d． 获取4G复用数据，获取规则如下
    SELECT DISTINCT CASE
    WHEN ND.CELLID < 128 THEN
      NVL(C1.PROVINCE_ID, '0')
    ELSE
    NVL(C2.PROVINCE_ID, '0')
    END PROVINCE_ID,
    CASE
    WHEN ND.CELLID < 128 THEN
      NVL(C1.CITY_ID, '0')
    ELSE
    NVL(C2.CITY_ID, '0')
    END CITY_ID,
    ND.LAC,
    ND.LAC_,
    ND.CELLID,
    CASE
    WHEN ND.CELLID < 128 THEN
      NVL(C1.TYPE, '05')
    ELSE
    NVL(C2.TYPE, '05')
    END NET_TYPE,
    1 STATUS
      FROM TEMP_LT_4G ND,
    (SELECT * FROM TEMP_LAC_INUSE_LT_4G_FY C1 WHERE C1.IF_REUSE = 0) C1,
    (SELECT * FROM TEMP_LAC_INUSE_LT_4G_FY C2 WHERE C2.IF_REUSE = 1) C2
      WHERE ND.LAC = C1.LAC(+)
    AND ND.LAC = C2.LAC(+)*/
   val jg_d= spark.sql(
      """
        |    SELECT DISTINCT CASE
        |    WHEN ND.CELLID < 128 THEN
        |      NVL(C1.PROVINCE_ID, '0')
        |    ELSE
        |    NVL(C2.PROVINCE_ID, '0')
        |    END PROVINCE_ID,
        |    CASE
        |    WHEN ND.CELLID < 128 THEN
        |      NVL(C1.CITY_ID, '0')
        |    ELSE
        |    NVL(C2.CITY_ID, '0')
        |    END CITY_ID,
        |    ND.LAC,
        |    ND.CELLID,
        |    CASE
        |    WHEN ND.CELLID < 128 THEN
        |      NVL(C1.TYPE, '05')
        |    ELSE
        |    NVL(C2.TYPE, '05')
        |    END NET_TYPE,
        |    1 STATUS
        |      FROM TEMP_LT_4G ND
        |      left join
        |    (SELECT * FROM TEMP_LAC_INUSE_LT_4G_FY C1 WHERE C1.IF_REUSE = 0) C1
        |    on ND.LAC = C1.LAC
        |    left join
        |    (SELECT * FROM TEMP_LAC_INUSE_LT_4G_FY C2 WHERE C2.IF_REUSE = 1) C2
        |      on ND.LAC = C2.LAC
      """.stripMargin)

    //e 获取2,3g
/*    SELECT
    DISTINCT NVL(CL.PROVINCE_ID, '0') PROVINCE_ID,
    NVL(CL.CITY_ID, '0') CITY_ID,
    ND.LAC,
    ND.LAC_,
    ND.CELLID,
    NVL(TYPE, '05') NET_TYPE,
    1 STATUS
      FROM TEMP_LT_23G ND, TEMP_LAC_INUSE_LT_23G CL
      WHERE ND.LAC = CL.LAC(+)*/
    val jg_e=spark.sql(
      """
        |SELECT
        |    DISTINCT NVL(CL.PROVINCE_ID, '0') PROVINCE_ID,
        |    NVL(CL.CITY_ID, '0') CITY_ID,
        |    ND.LAC,
        |    ND.CELLID,
        |    NVL(TYPE, '05') NET_TYPE,
        |    1 STATUS
        |      FROM TEMP_LT_23G ND left join TEMP_LAC_INUSE_LT_23G CL
        |      on ND.LAC = CL.LAC
      """.stripMargin)


    val Eighth_jg=(jg_a.union(jg_b).union(jg_c).union(jg_d).union(jg_e)).cache()
    Eighth_jg.checkpoint()

    //f 获取电信

/*    SELECT
    DISTINCT NVL(CL.PROVINCE_ID, '0') PROVINCE_ID,
    NVL(CL.CITY_ID, '0') CITY_ID,
    ND.LAC,
    ND.LAC_,
    ND.CELLID,
    NVL(TYPE, '05') NET_TYPE,
    1 STATUS
      FROM TEMP_DX_4G ND, TEMP_LAC_INUSE_DX_4G CL
      WHERE ND.LAC = CL.LAC(+);*/

//second_jg.coalesce(2).write.csv("hdfs://lisz1:9000/cs/second_tjg")
//写书据库
/*    Fourth_jg1.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection4: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement4: PreparedStatement = connection4
        .prepareStatement(s"insert into $oracl_tb41(province_code,city_code," +
          s"LAC,CELLID,NET_TYPE,NODE_TYPE,CELL_DATE,UPBYTES,DOWNBYTES," +
          s"SUMBYTES,CELL_COUNT,DURATION,STATION_ID,SECTION_ID," +
          s"CELLINFO_ID,CELL_NAME,STATUS,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,MAX_AVAILABLE_SPEED,MAX_VIDEO_SPEED," +
          s"MAX_FILE_SPEED,MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4,8,9,10,11,12)
        val arr_char=Array(1,2,5,6,7,13,14,15,16,17,18,19,20)
        val arr_nul=Array(21,22,23,24)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement4.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement4.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement4.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement4.setInt(k,0)
        }
        prepareStatement4.addBatch()
      })
      prepareStatement4.executeBatch()
      prepareStatement4.close()
      connection4.close()
    })
    Fourth_jg2.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection4: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement4: PreparedStatement = connection4
        .prepareStatement(s"insert into $oracl_tb42(province_code,city_code," +
          s"LAC,CELLID,NET_TYPE,NODE_TYPE,CELL_DATE,UPBYTES,DOWNBYTES," +
          s"SUMBYTES,CELL_COUNT,DURATION,STATION_ID,SECTION_ID," +
          s"CELLINFO_ID,CELL_NAME,STATUS,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,MAX_AVAILABLE_SPEED,MAX_VIDEO_SPEED," +
          s"MAX_FILE_SPEED,MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4,8,9,10,11,12)
        val arr_char=Array(1,2,5,6,7,13,14,15,16,17,18,19,20)
        val arr_nul=Array(21,22,23,24)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement4.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement4.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement4.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement4.setInt(k,0)
        }
        prepareStatement4.addBatch()
      })
      prepareStatement4.executeBatch()
      prepareStatement4.close()
      connection4.close()
    })
    Fourth_jg3.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection4: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement4: PreparedStatement = connection4
        .prepareStatement(s"insert into $oracl_tb43(province_code,city_code," +
          s"LAC,CELLID,NET_TYPE,NODE_TYPE,CELL_DATE,UPBYTES,DOWNBYTES," +
          s"SUMBYTES,CELL_COUNT,DURATION,STATION_ID,SECTION_ID," +
          s"CELLINFO_ID,CELL_NAME,STATUS,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,MAX_AVAILABLE_SPEED,MAX_VIDEO_SPEED," +
          s"MAX_FILE_SPEED,MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4,8,9,10,11,12)
        val arr_char=Array(1,2,5,6,7,13,14,15,16,17,18,19,20)
        val arr_nul=Array(21,22,23,24)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement4.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement4.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement4.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement4.setInt(k,0)
        }
        prepareStatement4.addBatch()
      })
      prepareStatement4.executeBatch()
      prepareStatement4.close()
      connection4.close()
    })

    Fifth_jg1.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection5: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement5: PreparedStatement = connection5
        .prepareStatement(s"insert into $oracl_tb51(province_code,city_code," +
          s"lac,cell_date,net_type," +
          s"node_type,upbytes,downbytes," +
          s"sumbytes,cell_count,duration," +
          s"pro_name,pro_stage,section_name," +
          s"section_id,station_id,status," +
          s"pms_code,max_available_speed," +
          s"max_video_speed,max_file_speed," +
          s"max_webbrowsing_speed)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,7,8,9,10,11)
        val arr_char=Array(1,2,4,5,6,12,13,14,15,16,17,18)
        val arr_nul=Array(19,20,21,22)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement5.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement5.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement5.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement5.setInt(k,0)
        }
        prepareStatement5.addBatch()
      })
      prepareStatement5.executeBatch()
      prepareStatement5.close()
      connection5.close()
    })
    Fifth_jg2.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection5: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement5: PreparedStatement = connection5
        .prepareStatement(s"insert into $oracl_tb52(province_code,city_code," +
          s"lac,cell_date,net_type," +
          s"node_type,upbytes,downbytes," +
          s"sumbytes,cell_count,duration," +
          s"pro_name,pro_stage,section_name," +
          s"section_id,station_id,status," +
          s"pms_code,max_available_speed," +
          s"max_video_speed,max_file_speed," +
          s"max_webbrowsing_speed)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,7,8,9,10,11)
        val arr_char=Array(1,2,4,5,6,12,13,14,15,16,17,18)
        val arr_nul=Array(19,20,21,22)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement5.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement5.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement5.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement5.setInt(k,0)
        }
        prepareStatement5.addBatch()
      })
      prepareStatement5.executeBatch()
      prepareStatement5.close()
      connection5.close()
    })
    Fifth_jg3.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection5: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement5: PreparedStatement = connection5
        .prepareStatement(s"insert into $oracl_tb53(province_code,city_code," +
          s"lac,cell_date,net_type," +
          s"node_type,upbytes,downbytes," +
          s"sumbytes,cell_count,duration," +
          s"pro_name,pro_stage,section_name," +
          s"section_id,station_id,status," +
          s"pms_code,max_available_speed," +
          s"max_video_speed,max_file_speed," +
          s"max_webbrowsing_speed)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,7,8,9,10,11)
        val arr_char=Array(1,2,4,5,6,12,13,14,15,16,17,18)
        val arr_nul=Array(19,20,21,22)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement5.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement5.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement5.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement5.setInt(k,0)
        }
        prepareStatement5.addBatch()
      })
      prepareStatement5.executeBatch()
      prepareStatement5.close()
      connection5.close()
    })


    Sixth_jg1.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection6: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement6: PreparedStatement = connection6
        .prepareStatement(s"insert into $oracl_tb61(province_code,city_code,LAC," +
          s"CELL_DATE,UPBYTES,DOWNBYTES,SUMBYTES," +
          s"CELL_COUNT,DURATION,STATION_NAME,NODE_CODE," +
          s"NET_TYPE,NODE_TYPE,LONGITUDE_PLAN,LATITUDE_PLAN," +
          s"LONGITUDE_BUILD,LATITUDE_BUILD,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,STATUS,STATION_ID,MAX_AVAILABLE_SPEED," +
          s"MAX_VIDEO_SPEED,MAX_FILE_SPEED," +
          s"MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,5,6,7,8,9,14,15,16,17)
        val arr_char=Array(1,2,4,10,11,12,13,18,19,20,21,22)
        val arr_nul=Array(23,24,25,26)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement6.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement6.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement6.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement6.setInt(k,0)
        }
        prepareStatement6.addBatch()
      })
      prepareStatement6.executeBatch()
      prepareStatement6.close()
      connection6.close()
    })
    Sixth_jg2.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection6: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement6: PreparedStatement = connection6
        .prepareStatement(s"insert into $oracl_tb62(province_code,city_code,LAC," +
          s"CELL_DATE,UPBYTES,DOWNBYTES,SUMBYTES," +
          s"CELL_COUNT,DURATION,STATION_NAME,NODE_CODE," +
          s"NET_TYPE,NODE_TYPE,LONGITUDE_PLAN,LATITUDE_PLAN," +
          s"LONGITUDE_BUILD,LATITUDE_BUILD,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,STATUS,STATION_ID,MAX_AVAILABLE_SPEED," +
          s"MAX_VIDEO_SPEED,MAX_FILE_SPEED," +
          s"MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,5,6,7,8,9,14,15,16,17)
        val arr_char=Array(1,2,4,10,11,12,13,18,19,20,21,22)
        val arr_nul=Array(23,24,25,26)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement6.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement6.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement6.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement6.setInt(k,0)
        }
        prepareStatement6.addBatch()
      })
      prepareStatement6.executeBatch()
      prepareStatement6.close()
      connection6.close()
    })
    Sixth_jg3.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection6: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement6: PreparedStatement = connection6
        .prepareStatement(s"insert into $oracl_tb63(province_code,city_code,LAC," +
          s"CELL_DATE,UPBYTES,DOWNBYTES,SUMBYTES," +
          s"CELL_COUNT,DURATION,STATION_NAME,NODE_CODE," +
          s"NET_TYPE,NODE_TYPE,LONGITUDE_PLAN,LATITUDE_PLAN," +
          s"LONGITUDE_BUILD,LATITUDE_BUILD,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,STATUS,STATION_ID,MAX_AVAILABLE_SPEED," +
          s"MAX_VIDEO_SPEED,MAX_FILE_SPEED," +
          s"MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,5,6,7,8,9,14,15,16,17)
        val arr_char=Array(1,2,4,10,11,12,13,18,19,20,21,22)
        val arr_nul=Array(23,24,25,26)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement6.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement6.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement6.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement6.setInt(k,0)
        }
        prepareStatement6.addBatch()
      })
      prepareStatement6.executeBatch()
      prepareStatement6.close()
      connection6.close()
    })


    Seventh_jg.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection7: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement7: PreparedStatement = connection7
        .prepareStatement(s"insert into $oracl_tb7(lac,lac_,cellid,sumbytes,province_id)values(?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(1,2,3,4)
        val arr_char=Array(5)
//        val arr_nul=Array(14,15,16,17)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim.toLowerCase) != "null" && !(row.get(i-1).toString.endsWith("E"))  ){
          prepareStatement7.setDouble(i, row.get(i-1).toString.toDouble)
        }else{
          prepareStatement7.setDouble(i,0)
        }
        }
        for (j <- arr_char){

          if(row.get(j-1) != null && (row.get(j-1).toString.trim) != "" && row.get(j-1).toString.toLowerCase != "null"  ){
            prepareStatement7.setString(j, row.get(j - 1).toString)
          }else{
            prepareStatement7.setString(j,"")
          }
        }

//        for(k <- arr_nul){
//          prepareStatement.setInt(k,0)
//        }
        prepareStatement7.addBatch()
      })
      prepareStatement7.executeBatch()
      prepareStatement7.close()
      connection7.close()
    })
    Eighth_jg.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection8: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement8: PreparedStatement = connection8
        .prepareStatement(s"insert into $oracl_tb8(PROVINCE_ID,CITY_ID," +
          s"LAC,LAC_ ,CELLID,NET_TYPE,STATUS)values(?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4)
        val arr_char=Array(1,2)
        val arr_char2=Array(5,6,7)

        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement8.setDouble(i, row.get(2).toString.toDouble)
          }else{
            prepareStatement8.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement8.setString(j,row.get(j-1).toString)
        }
        for (j <- arr_char2){
          prepareStatement8.setString(j,row.get(j-2).toString)
        }

        prepareStatement8.addBatch()
      })
      prepareStatement8.executeBatch()
      prepareStatement8.close()
      connection8.close()
    })*/


// last jg in oralce

/*    first_jg.repartition(100).foreachPartition(rows => {
      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection1: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement1: PreparedStatement = connection1
        .prepareStatement(s"insert into $oracl_tb1(province_code,city_code, " +
          s"lac, cellid, net_type, node_type, cell_date, cell_hour,station_id,section_id, cellinfo_id," +
          s"cell_name,status,pro_stage,pro_name, pms_code, upbytes," +
          s" downbytes, sumbytes, cell_count, duration," +
          s"max_available_speed, max_video_speed, max_file_speed, " +
          s"max_webbrowsing_speed)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4,17,18,19,20,21)
        val arr_char=Array(1,2,5,6,7,8,14,15,16,9,10,11,12,13)
        val arr_nul=Array(22,23,24,25)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement1.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement1.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement1.setString(j,row.get(j-1).toString)

        }


        for(k <- arr_nul){
          prepareStatement1.setInt(k,0)
        }

        prepareStatement1.addBatch()
      })
      prepareStatement1.executeBatch()
      prepareStatement1.close()
      connection1.close()
    })

    second_jg.repartition(100).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection2: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement2: PreparedStatement = connection2
        .prepareStatement(s"insert into $oracl_tb2(province_code,city_code," +
          s"lac,cell_date,net_type,node_type,cell_hour,upbytes,downbytes," +
          s"sumbytes,cell_count,duration,station_id," +
          s"section_id,status,section_name,pro_stage," +
          s"pro_name,pms_code,max_available_speed,max_video_speed," +
          s"max_file_speed,max_webbrowsing_speed) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,8,9,10,11,12)
        val arr_char=Array(1,2,4,5,6,7,13,14,15,16,17,18,19)
        val arr_nul=Array(20,21,22,23)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement2.setDouble(i, row.get(i-1).toString.toDouble)
        }else{
          prepareStatement2.setDouble(i,0)
        }
        }
        for (j <- arr_char){
          prepareStatement2.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement2.setInt(k,0)
        }
        prepareStatement2.addBatch()
      })
      prepareStatement2.executeBatch()
      prepareStatement2.close()
      connection2.close()
    })

    third_jg.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection3: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement3: PreparedStatement = connection3
        .prepareStatement(s"insert into $oracl_tb3(province_code,city_code,lac,cell_date," +
          s"cell_hour,net_type,upbytes,downbytes,sumbytes," +
          s"cell_count,duration,station_id,status,station_name," +
          s"node_code,node_type,longitude_plan,latitude_plan," +
          s"longitude_build,latitude_build,pro_stage,pro_name," +
          s"pms_code,max_available_speed,max_video_speed," +
          s"max_file_speed,max_webbrowsing_speed) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,7,8,9,10,11,17,18,19,20)
        val arr_char=Array(1,2,4,5,6,12,13,14,15,16,21,22,23)
        val arr_nul=Array(24,25,26,27)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement3.setDouble(i, (row.get(i-1).toString.trim).toDouble)
          }else{
            prepareStatement3.setDouble(i,0)
          }

        }
        for (j <- arr_char){
          prepareStatement3.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement3.setInt(k,0)
        }
        prepareStatement3.addBatch()
      })
      prepareStatement3.executeBatch()
      prepareStatement3.close()
      connection3.close()
    })*/
//         data1.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/data1_jg")
//        cell_f1.write.csv("hdfs://lisz1:9000/cs/20180715/cellf1_jg")
//        cell_f2.write.csv("hdfs://lisz1:9000/cs/20180715/cellf2_jg")
//        cell_f3.write.csv("hdfs://lisz1:9000/cs/20180715/cellf3_jg")
//        first_jg.write.csv(s"hdfs://lisz1:9000/cs/${McsSf}/first_jg")
/*        first_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/data1/hivetb/llqsq/first_jg")
        second_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/data1/hivetb/llqsq/second_jg")
        third_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/data1/hivetb/llqsq/third_jg")*/
    first_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/first_jg")
    second_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/second_jg")
    third_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/third_jg")
//一下测试

/*        val aa = spark.sql("""select * from cell_tm1""")
    val bb = spark.sql("""select * from cell_tm2""")
    val cc = spark.sql("""select * from cell_tm3""")

    aa.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/cell_tm1")
    bb.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/cell_tm2")
    cc.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/cell_tm3")


        data_p4g.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/data_p4g")
        data_p3g.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/data_p3g")
        data_p2g.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/data_p2g")

        xq3.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/xq3")
        xq2.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/xq2")
        xq4.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/qg/xq4")*/

    //        Fifth_jg2.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/fourth2_jg")
        Fourth_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/fourth_jg")
        Fifth_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/Fifth_jg")
        Sixth_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/Sixth_jg")

        Seventh_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/Seventh_jg")
        Eighth_jg.write.option("delimiter", "\t").csv(s"hdfs://lisz1:9000/cs/${McsSf}/Eighth_jg")



//    data1.write.csv("hdfs://lisz1:9000/cs/20180715/data1_jg")
//    cell_f1.write.csv("hdfs://lisz1:9000/cs/20180715/cellf1_jg")
//    cell_f2.write.csv("hdfs://lisz1:9000/cs/20180715/cellf2_jg")
//    cell_f3.write.csv("hdfs://lisz1:9000/cs/20180715/cellf3_jg")
//    first_jg.write.csv("hdfs://lisz1:9000/cs/20180715/first_jg")




//        Eighth_jg.write.csv("hdfs://lisz1:9000/cs/20180715/Eighth_jg")


/*    Fourth_jg1.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection4: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement4: PreparedStatement = connection4
        .prepareStatement(s"insert into $oracl_tb41(province_code,city_code," +
          s"LAC,CELLID,NET_TYPE,NODE_TYPE,CELL_DATE,UPBYTES,DOWNBYTES," +
          s"SUMBYTES,CELL_COUNT,DURATION,STATION_ID,SECTION_ID," +
          s"CELLINFO_ID,CELL_NAME,STATUS,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,MAX_AVAILABLE_SPEED,MAX_VIDEO_SPEED," +
          s"MAX_FILE_SPEED,MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4,8,9,10,11,12)
        val arr_char=Array(1,2,5,6,7,13,14,15,16,17,18,19,20)
        val arr_nul=Array(21,22,23,24)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement4.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement4.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement4.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement4.setInt(k,0)
        }
        prepareStatement4.addBatch()
      })
      prepareStatement4.executeBatch()
      prepareStatement4.close()
      connection4.close()
    })
    Fourth_jg2.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection4: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement4: PreparedStatement = connection4
        .prepareStatement(s"insert into $oracl_tb42(province_code,city_code," +
          s"LAC,CELLID,NET_TYPE,NODE_TYPE,CELL_DATE,UPBYTES,DOWNBYTES," +
          s"SUMBYTES,CELL_COUNT,DURATION,STATION_ID,SECTION_ID," +
          s"CELLINFO_ID,CELL_NAME,STATUS,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,MAX_AVAILABLE_SPEED,MAX_VIDEO_SPEED," +
          s"MAX_FILE_SPEED,MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4,8,9,10,11,12)
        val arr_char=Array(1,2,5,6,7,13,14,15,16,17,18,19,20)
        val arr_nul=Array(21,22,23,24)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement4.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement4.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement4.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement4.setInt(k,0)
        }
        prepareStatement4.addBatch()
      })
      prepareStatement4.executeBatch()
      prepareStatement4.close()
      connection4.close()
    })

    Fifth_jg1.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection5: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement5: PreparedStatement = connection5
        .prepareStatement(s"insert into $oracl_tb51(province_code,city_code," +
          s"lac,cell_date,net_type," +
          s"node_type,upbytes,downbytes," +
          s"sumbytes,cell_count,duration," +
          s"pro_name,pro_stage,section_name," +
          s"section_id,station_id,status," +
          s"pms_code,max_available_speed," +
          s"max_video_speed,max_file_speed," +
          s"max_webbrowsing_speed)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,7,8,9,10,11)
        val arr_char=Array(1,2,4,5,6,12,13,14,15,16,17,18)
        val arr_nul=Array(19,20,21,22)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement5.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement5.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement5.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement5.setInt(k,0)
        }
        prepareStatement5.addBatch()
      })
      prepareStatement5.executeBatch()
      prepareStatement5.close()
      connection5.close()
    })
    Fifth_jg2.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection5: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement5: PreparedStatement = connection5
        .prepareStatement(s"insert into $oracl_tb52(province_code,city_code," +
          s"lac,cell_date,net_type," +
          s"node_type,upbytes,downbytes," +
          s"sumbytes,cell_count,duration," +
          s"pro_name,pro_stage,section_name," +
          s"section_id,station_id,status," +
          s"pms_code,max_available_speed," +
          s"max_video_speed,max_file_speed," +
          s"max_webbrowsing_speed)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,7,8,9,10,11)
        val arr_char=Array(1,2,4,5,6,12,13,14,15,16,17,18)
        val arr_nul=Array(19,20,21,22)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement5.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement5.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement5.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement5.setInt(k,0)
        }
        prepareStatement5.addBatch()
      })
      prepareStatement5.executeBatch()
      prepareStatement5.close()
      connection5.close()
    })


    Sixth_jg1.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection6: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement6: PreparedStatement = connection6
        .prepareStatement(s"insert into $oracl_tb61(province_code,city_code,LAC," +
          s"CELL_DATE,UPBYTES,DOWNBYTES,SUMBYTES," +
          s"CELL_COUNT,DURATION,STATION_NAME,NODE_CODE," +
          s"NET_TYPE,NODE_TYPE,LONGITUDE_PLAN,LATITUDE_PLAN," +
          s"LONGITUDE_BUILD,LATITUDE_BUILD,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,STATUS,STATION_ID,MAX_AVAILABLE_SPEED," +
          s"MAX_VIDEO_SPEED,MAX_FILE_SPEED," +
          s"MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,5,6,7,8,9,14,15,16,17)
        val arr_char=Array(1,2,4,10,11,12,13,18,19,20,21,22)
        val arr_nul=Array(23,24,25,26)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement6.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement6.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement6.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement6.setInt(k,0)
        }
        prepareStatement6.addBatch()
      })
      prepareStatement6.executeBatch()
      prepareStatement6.close()
      connection6.close()
    })
    Sixth_jg2.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection6: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement6: PreparedStatement = connection6
        .prepareStatement(s"insert into $oracl_tb62(province_code,city_code,LAC," +
          s"CELL_DATE,UPBYTES,DOWNBYTES,SUMBYTES," +
          s"CELL_COUNT,DURATION,STATION_NAME,NODE_CODE," +
          s"NET_TYPE,NODE_TYPE,LONGITUDE_PLAN,LATITUDE_PLAN," +
          s"LONGITUDE_BUILD,LATITUDE_BUILD,PRO_STAGE,PRO_NAME," +
          s"PMS_CODE,STATUS,STATION_ID,MAX_AVAILABLE_SPEED," +
          s"MAX_VIDEO_SPEED,MAX_FILE_SPEED," +
          s"MAX_WEBBROWSING_SPEED)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,5,6,7,8,9,14,15,16,17)
        val arr_char=Array(1,2,4,10,11,12,13,18,19,20,21,22)
        val arr_nul=Array(23,24,25,26)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
            prepareStatement6.setDouble(i, row.get(i-1).toString.toDouble)
          }else{
            prepareStatement6.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement6.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement6.setInt(k,0)
        }
        prepareStatement6.addBatch()
      })
      prepareStatement6.executeBatch()
      prepareStatement6.close()
      connection6.close()
    })


    Seventh_jg.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection7: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement7: PreparedStatement = connection7
        .prepareStatement(s"insert into $oracl_tb7(lac,lac_,cellid,sumbytes,province_id)values(?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(1,2,4)
        val arr_char=Array(3,5)
//        val arr_nul=Array(14,15,16,17)
        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement7.setDouble(i, row.get(i-1).toString.toDouble)
        }else{
          prepareStatement7.setDouble(i,0)
        }
        }
        for (j <- arr_char){
          prepareStatement7.setString(j,row.get(j-1).toString)
        }

//        for(k <- arr_nul){
//          prepareStatement.setInt(k,0)
//        }
        prepareStatement7.addBatch()
      })
      prepareStatement7.executeBatch()
      prepareStatement7.close()
      connection7.close()
    })

    Eighth_jg.repartition(50).foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection8: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement8: PreparedStatement = connection8
        .prepareStatement(s"insert into $oracl_tb8(PROVINCE_ID,CITY_ID," +
          s"LAC,LAC_ ,CELLID,NET_TYPE,STATUS)values(?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4)
        val arr_char=Array(1,2)
        val arr_char2=Array(5,6,7)

        for (i <- arr_nm){
          if((row.get(i-1).toString.trim) != "null"){
          prepareStatement8.setDouble(i, row.get(2).toString.toDouble)
          }else{
            prepareStatement8.setDouble(i,0)
          }
        }
        for (j <- arr_char){
          prepareStatement8.setString(j,row.get(j-1).toString)
        }
        for (j <- arr_char2){
          prepareStatement8.setString(j,row.get(j-2).toString)
        }

        prepareStatement8.addBatch()
      })
      prepareStatement8.executeBatch()
      prepareStatement8.close()
      connection8.close()
    })*/

/*    first_jg.createOrReplaceTempView("lsz_0716f")
    val first_jg1 = spark.sql(s""" select * from lsz_0716f where cell_date = '${ystd}' """)

    first_jg1.repartition(150).foreachPartition { records =>
      val config = HBaseConfiguration.create
      config.set("hbase.zookeeper.property.clientPort", "2181")
      config.set("hbase.zookeeper.quorum", "lisz1,lisz9,lisz10,lisz11,lisz12")
      val connection = ConnectionFactory.createConnection(config)
      val table = connection.getTable(TableName.valueOf("tb2"))

      // 举个例子而已，真实的代码根据records来
      val list = new java.util.ArrayList[Put]
      //      for(i <- 0 until 10){
      //        val put = new Put(Bytes.toBytes(i.toString))
      //        put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("aaaa"), Bytes.toBytes("1111"))
      //        list.add(put)
      //      }
      records.foreach(row =>{
        val put =new Put(Bytes.toBytes(row.get(10)+"_"+row.get(7)+"_"+row.get(6)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(row.get(0)+","+row.get(1)+","+row.get(2)+","
          +row.get(3)+","+row.get(4)+"_"+row.get(5)+"_"+row.get(6)+"_"+row.get(7)+"_"+row.get(8)
          +"_"+row.get(9)+"_"+row.get(10)+"_"+row.get(11)+"_"+row.get(12)+"_"+row.get(13)+"_"+row.get(14)+
          "_"+row.get(15)+"_"+row.get(16)+"_"+row.get(17)+"_"+row.get(18)+"_"+row.get(19)+"_"+row.get(20)))
        list.add(put)
      })

      // 批量提交
      table.put(list)
      // 分区数据写入HBase后关闭连接
      table.close()
    }

    second_jg.createOrReplaceTempView("lsz_0716s")
    val second_jg1 = spark.sql(s""" select * from lsz_0716s where cell_date = '${ystd}' """)

    second_jg1.repartition(150).foreachPartition { records =>
      val config = HBaseConfiguration.create
      config.set("hbase.zookeeper.property.clientPort", "2181")
      config.set("hbase.zookeeper.quorum", "lisz1,lisz9,lisz10,lisz11,lisz12")
      val connection = ConnectionFactory.createConnection(config)
      val table = connection.getTable(TableName.valueOf("tb3"))

      // 举个例子而已，真实的代码根据records来
      val list = new java.util.ArrayList[Put]
      //      for(i <- 0 until 10){
      //        val put = new Put(Bytes.toBytes(i.toString))
      //        put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("aaaa"), Bytes.toBytes("1111"))
      //        list.add(put)
      //      }
      records.foreach(row =>{
        val put =new Put(Bytes.toBytes(row.get(13)+"_"+row.get(6)+"_"+row.get(3)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(row.get(0)+","+row.get(1)+","+row.get(2)+","
          +row.get(3)+","+row.get(4)+"_"+row.get(5)+"_"+row.get(6)+"_"+row.get(7)+"_"+row.get(8)
          +"_"+row.get(9)+"_"+row.get(10)+"_"+row.get(11)+"_"+row.get(12)+"_"+row.get(13)+"_"+row.get(14)+
          "_"+row.get(15)+"_"+row.get(16)+"_"+row.get(17)+"_"+row.get(18)))
        list.add(put)
      })

      // 批量提交
      table.put(list)
      // 分区数据写入HBase后关闭连接
      table.close()
    }

    third_jg.createOrReplaceTempView("lsz_0716t")
    val third_jg1 = spark.sql(s""" select * from lsz_0716t where cell_date = '${ystd}' """)

    third_jg1.repartition(150).foreachPartition { records =>
      val config = HBaseConfiguration.create
      config.set("hbase.zookeeper.property.clientPort", "2181")
      config.set("hbase.zookeeper.quorum", "lisz1,lisz9,lisz10,lisz11,lisz12")
      val connection = ConnectionFactory.createConnection(config)
      val table = connection.getTable(TableName.valueOf("tb4"))

      // 举个例子而已，真实的代码根据records来
      val list = new java.util.ArrayList[Put]
      //      for(i <- 0 until 10){
      //        val put = new Put(Bytes.toBytes(i.toString))
      //        put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("aaaa"), Bytes.toBytes("1111"))
      //        list.add(put)
      //      }
      records.foreach(row =>{
        val put =new Put(Bytes.toBytes(row.get(11)+"_"+row.get(4)+"_"+row.get(3)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(row.get(0)+","+row.get(1)+","+row.get(2)+","
          +row.get(3)+","+row.get(4)+"_"+row.get(5)+"_"+row.get(6)+"_"+row.get(7)+"_"+row.get(8)
          +"_"+row.get(9)+"_"+row.get(10)+"_"+row.get(11)+"_"+row.get(12)+"_"+row.get(13)+"_"+row.get(14)+
          "_"+row.get(15)+"_"+row.get(16)+"_"+row.get(17)+"_"+row.get(18)+"_"+row.get(19)+"_"+row.get(20)
          +"_"+row.get(21)+"_"+row.get(22)))
        list.add(put)
      })

      // 批量提交
      table.put(list)
      // 分区数据写入HBase后关闭连接
      table.close()
    }*/

/*
first_jg.foreachPartition(rows => {

      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      val connection: Connection = DriverManager.getConnection(url, user, password)
      val prepareStatement: PreparedStatement = connection
        .prepareStatement(s"insert into $oracl_tb1(province_id,city_id, " +
          s"lac, cellid, net_type, node_type, cell_date, cell_hour, upbytes," +
          s" downbytes, sumbytes, cell_count, duration, station_id, section_id," +
          s" cellinfo_id, cell_name, status, pro_stage, pro_name, pms_code, " +
          s"max_available_speed, max_video_speed, max_file_speed, " +
          s"max_webbrowsing_speed)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      rows.foreach(row => {
        val arr_nm=Array(3,4,9,10,11,12,13)
        val arr_char=Array(1,2,5,6,7,8,14,15,16,17,18,19,20,21)
        val arr_nul=Array(22,23,24,25)
        for (i <- arr_nm){

          prepareStatement.setInt(i, row.get(i-1).toString.toInt)
        }
        for (j <- arr_char){
          prepareStatement.setString(j,row.get(j-1).toString)
        }

        for(k <- arr_nul){
          prepareStatement.setInt(k,0)
        }
        prepareStatement.addBatch()
      })
      prepareStatement.executeBatch()
      prepareStatement.close()
      connection.close()
    })
*/

    //    data_p2g.join(data_p3g,["name,String"],)
/*      spark.sql(
      s
        |select * from temp1
        |where lac<0;
      """.stripMargin)*/
//    data_p2g.repartition(1).write.csv("hdfs://jacky:9000/cs/jg")
//    srcRdd.saveAsTextFile("hdfs://jacky:9000/cs/jg")
//    srcRdd.repartition(1).csv("hdfs://jacky:9000/cs/jg")
spark.close()
spark.stop()

  }


  def ismInt(str:String):Int= {

    if (str.toDouble<2147483647) {
      str.trim.toInt
    } else {
      0
    }

  }

  def ismDouble(str:String):Double={
    if(str == "null" || str.isEmpty){
      0.0
    }else{
      str.trim.toDouble
    }
  }
  //检验数据的合格性
  //    '1280','19202','08','77','75432','64736','140168','6971909','20180314' 2018-03-04
  def inspect(str: Array[String]): Boolean = {
    if (str.length == 10) {
      val sttime= str(9).replace("'","").trim
      val t1 = sttime.trim.size
      val hour=str(3).trim
      val provc = str(0).trim
      if(t1==10 && Mmatch(sttime) && sttime.endsWith("2018-07-15")){
        if(isIntByRegex(str(1)) && isIntByRegex(str(2))  && ispro(provc)){
          true
        }else{
          false
        }
      }else{
        false
      }

      //        val lac=str(6).trim.size
      //        val ci=str(7).trim.size

    } else {
      false
    }
  }
//is or not double
def isIntByRegex(s : String) = {
  //    val pattern = """^(\d+)$""".r
  val pattern = """^-?(\d+)?\.?(\d+)E?(\d+)$""".r
  s match {
    case pattern(_*) => true
    case _ => false
  }
}

/*def isIntByRegex(s : String) = {
  //    val pattern = """^(\d+)$""".r
  val pattern = """^-?(\d+)?\.?(\d+)$""".r
  s match {
    case pattern(_*) => true
    case _ => false
  }
}*/

  // 获取当前时间
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.format(now)
    return date
  }

  def fun (data1:String,date2:String):Long={
    val sdf =new SimpleDateFormat("yyyyMMdd")
    println(sdf.parse(data1).getTime)
    println(sdf.parse(date2))
    val b=sdf.parse(data1).getTime-sdf.parse(date2).getTime
    val num=b/(1000*3600*24)
    num
  }

  //match date yyyy'

  def kkfilter(z: Array[String]): Boolean ={
    if(z.size<9){
      false
    }else{
      val lac = z(1)
      val ci = z(2)
      if(lac.toString.trim.startsWith("--") || ci.toString.trim.startsWith("-") ){
        false
      }else{
        true
      }
    }

  }

  def Mmatch(dt:String):Boolean={
    val mm = dt.trim.substring(0,4)
    //    println(mm)
    val papa = """(^201[8-9])""".r
    //    val papa = """(\d{4})(\d{2})(\d{2})""".r
    mm match {
      case papa(_*) => true
      case _ => false
    }
  }

  def getYestermo():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def getYesterday():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def getYesterday2():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def getYesterday3():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -3)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def getystd():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def getystd2():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def getystd3():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -3)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

//20180321 ==> 2018-03-21
  def zhHd(jg:String):String={
    if(jg.size==8){
      val yy =jg.substring(0,4)
      val mm =jg.substring(4,6)
      val dd = jg.substring(6,8)
      val ls = s"$yy-$mm-$dd"
      ls
    }else{
      "0000-00-00"
    }
  }

  def isHour(str:String) = {
    val site: List[String] = List("00", "01", "02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24")
    if(site.contains(str)){
      true
    }else{
      false
    }
  }


//  def ispro(str:String) = {
//    val site: List[String] = List("010", "011", "013","017","018","019","030","031","034","036","038","050",
//      "051","059","070","071","074","075","076","079","081","083","084","085","086","087","088","089",
//      "090","091","097")
//    if(site.contains(str)){
//      true
//    }else{
//      false
//    }
//  }
  def ispro(str:String) = {
    val site: List[String] = List("10", "11", "13","17","18","19","30","31","34","36","38","50",
      "51","59","70","71","74","75","76","79","81","83","84","85","86","87","88","89",
      "90","91","97")
    if(site.contains(str)){
      true
    }else{
      false
    }
  }

}
