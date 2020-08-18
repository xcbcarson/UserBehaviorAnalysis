package com.caron

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @author Caron
 * @create 2020-08-14-21:37
 * @Description ${description}
 * @Version $version
 */
//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
// 定义输出pv统计的样例类
case class PvCount(windowEnd: Long, count: Long)
object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

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
      //.map(x => ("pv", 1L))
      .map(new MyMapper())
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      //.sum(1)
      .aggregate(new PvCountAgg(), new PvCountWindowResult())
      .keyBy(_.windowEnd)
      .process(new TotalPvCountResult())
      .print()

    env.execute()
  }
}

class PvCountAgg() extends AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class PvCountWindowResult() extends WindowFunction[Long,PvCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd,input.head))
    //out.collect(PvCount(window.getEnd,input.toIterator.next()))
  }
}
// 自定义mapper，随机生成分组的key
class MyMapper extends MapFunction[UserBehavior,(String,Long)]{
  override def map(t: UserBehavior): (String, Long) = {
    (Random.nextString(10),1L)
  }
}

class TotalPvCountResult extends KeyedProcessFunction[Long,PvCount,PvCount]{
  lazy val totalPvCountResultState : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-pv",classOf[Long]))

  override def processElement(i: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
    totalPvCountResultState.update(totalPvCountResultState.value() + i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    val totalPvCount = totalPvCountResultState.value()
    out.collect(PvCount(ctx.getCurrentKey,totalPvCount))
    totalPvCountResultState.clear()
  }
}