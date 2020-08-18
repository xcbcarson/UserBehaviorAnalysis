package com.caron.market_analysis

import java.sql.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author Caron
 * @create 2020-08-17-16:25
 * @Description ${description}
 * @Version $version
 */
case class AdClickLog(userId :Long ,addId:Long,province:String,city:String,timestamp:Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream = env.readTextFile(resource.getPath)


    val dataStream = inputStream.map(
      data=>{
        val arr = data.split(",")
        AdClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L)

    val adCountStream = dataStream
      .keyBy(_.province)
        .timeWindow(Time.hours(1),Time.seconds(5))
        .aggregate(new AdCount(),new CountView())
        .print()


    env.execute("adclick")
  }
}

class AdCount() extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class CountView() extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString,key,input.head))
  }
}