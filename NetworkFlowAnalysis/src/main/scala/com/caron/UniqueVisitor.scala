package com.caron

import com.caron.PageView.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author Caron
 * @create 2020-08-15-9:41
 * @Description ${description}
 * @Version $version
 */
case class UvCount(windowEnd: Long, count: Long)
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream = env.readTextFile(resource.getPath)

    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt,arr(3), arr(4).toLong)
      }
    )
      .assignAscendingTimestamps(_.timestamp * 1000L) //转换为毫秒
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
        .apply(new UvCountResult())
        .print()

    env.execute()
  }
}
//自定义实现全窗口函数，set集合自动去重
class UvCountResult() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var userIdSet = Set[Long]()
    for (userBehavior <- input){
      userIdSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd,userIdSet.size))
  }
}