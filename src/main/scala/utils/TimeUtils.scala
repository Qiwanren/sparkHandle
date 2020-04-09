package utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtils {

  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar = Calendar.getInstance()

  def apply(time: String) = {
    calendar.setTime(simpleDateFormat.parse(time))
    calendar.getTimeInMillis
  }

  /***********
    * 过滤某个时间段的时间值
    * @param time
    * @param startTime
    * @param endTime
    * @return
    */
  def DateFilter(time:String,startTime:Long,endTime:Long): Boolean ={
    val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")
    val logTime = dateFormat.parse(time).getTime
    return logTime >= startTime && logTime < endTime
  }

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
    //println(StringUtils("qwr"))
    handleDate("2016年2月1日,星期一,10:01:37")

  }

}
