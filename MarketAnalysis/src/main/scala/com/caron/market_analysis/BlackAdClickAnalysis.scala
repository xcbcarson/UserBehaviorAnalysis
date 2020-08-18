package com.caron.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
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
// 侧输出流黑名单报警信息样例类
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

object BlackAdClickAnalysis {
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

    val filterBlackUser = dataStream.keyBy(
      data => {
        (data.userId,data.addId)
      }
    )
      .process(new FilterBlackUser(100))

    val adCountStream = dataStream
      .keyBy(_.province)
        .timeWindow(Time.hours(1),Time.seconds(5))
        .aggregate(new AdCount(),new CountView())
        .print("count result")
    filterBlackUser.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("waring")

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

class FilterBlackUser(n : Int) extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog]{
  lazy val countState : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
  lazy val resetTimerTsState : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reste-ts",classOf[Long]))
  lazy val isBlackState : ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black",classOf[Boolean]))

  override def processElement(i: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    val curCount = countState.value()
    if(curCount == 0){
      val ts = (context.timerService().currentProcessingTime()/(1000*24*60*60) + 1)*(24*1000*60*60)-8*60*60*1000
      resetTimerTsState.update(ts)
      context.timerService().registerProcessingTimeTimer(ts)
    }

    if(curCount >= n){
      if(!isBlackState.value()){
        isBlackState.update(true)
        context.output(new OutputTag[BlackListUserWarning]("warning"),BlackListUserWarning(i.userId,i.addId,"Click ad over " + n + " times today!!!"))
      }
      return
    }
    // 正常情况，count加1，然后将数据原样输出
    countState.update(curCount + 1)
    out.collect(i)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if(timestamp == resetTimerTsState.value()){
      isBlackState.clear()
      countState.clear()
    }
  }
}