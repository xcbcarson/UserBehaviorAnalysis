package com.caron.hotItemsanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author Caron
 * @create 2020-08-13-16:06
 * @Description ${description}
 * @Version $version
 */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("E:\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt,arr(3).toString, arr(4).toLong)
      }
    )
      .assignAscendingTimestamps(_.timestamp * 1000L) //转换为毫秒
    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5)) //设置滑动窗口
      .aggregate(new CountAgg(), new ItemViewWindowResult())
    val result: DataStream[String] = aggStream
      .keyBy("windowEnd") //按照窗口分组，收集当前窗口商品的count值
      .process(new TopNHotItems(5))
    result.print()


    env.execute()
  }
}
//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  //初始化
  override def createAccumulator(): Long = 0L
//每条数据进入调用一次
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数
class  ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.toIterator.next()
    out.collect(ItemViewCount(itemId,windowEnd,count))
  }
}

//自定义处理函数
class  TopNHotItems(topSize:Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{
  //先定义状态
  private var itemViewCountListState : ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new
        ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //每来一条数据，直接加入liststate
    itemViewCountListState.add(value)
    //注册一个windowEnd + 1 之后触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }
  //按定时器触发，可以排序输出了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItemViewCounts : ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while(iter.hasNext){
      allItemViewCounts += iter.next()
    }
    //清空状态
    itemViewCountListState.clear()
    //按照count大小排序
    val sortedItemViewCounts = allItemViewCounts
      .sortBy(_.count)(Ordering.Long.reverse) //倒序
      .take(topSize)

    //将排名信息格式化为String
    val result = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    //遍历结果列表中的每个ItemViewCount,输出到一行
    for(i <- sortedItemViewCounts.indices){  //indices省略0 to ...
      val currentChild = sortedItemViewCounts(i)
      result.append("NO").append(i+1).append(":")
        .append("商品ID = ").append(currentChild.itemId).append("\t")
        .append("热门度 = ").append(currentChild.count).append("\n")
    }
    result.append("========================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

















