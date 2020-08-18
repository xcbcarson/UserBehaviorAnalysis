package com.caron

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author Caron
 * @create 2020-08-14-11:21
 * @Description ${description}
 * @Version $version
 */
//定义输入数据样例类
case class ApacheLogEvent(ip:String, userId:String, timestamp :Long , method:String, url :String)

case class PageViewCount(url:String, windowEnd:Long, count:Long)
object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("E:\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val dataStream: DataStream[ApacheLogEvent] = inputStream.map(
      data => {
        val arr = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
      }
    )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.timestamp
        }
      })
    //进行开窗聚合以及排序输出
    val aggStream = dataStream
      .filter(_.method == "GET")
      //.keyBy("url")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1)) //一分钟内的数据继续计算
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late")) //计算一分钟外的
      .aggregate(new PageCountAgg, new PageViewCountWindowResult())

    val resultStream = aggStream.keyBy(_.windowEnd).process(new TopHotPage(3))

    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")
    resultStream.print()

    env.execute("page test")

  }
}

class PageCountAgg extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class PageViewCountWindowResult extends WindowFunction[Long,PageViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key,window.getEnd,input.toIterator.next() ) )
  }
}
class TopHotPage(n:Int) extends KeyedProcessFunction[Long,PageViewCount, String]{
  //lazy val pageViewCountListState : ListState[PageViewCount] = getRuntimeContext.getListState(
    //new ListStateDescriptor[PageViewCount]("pageCount-list",classOf[PageViewCount]))
  lazy val pageViewCountMapState : MapState[String,Long] = getRuntimeContext.getMapState(
      new MapStateDescriptor[String,Long]("pageCount-map",classOf[String],classOf[Long]))

  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
    //pageViewCountListState.add(i)
    pageViewCountMapState.put(i.url,i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    //另外注册一个定时器，此时窗口关闭,一分钟后触发，清空状态
    context.timerService().registerEventTimeTimer(i.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //val allPageViewCounts:ListBuffer[PageViewCount] = ListBuffer()
    /*val iter = pageViewCountListState.get().iterator()
    while(iter.hasNext){
      allPageViewCounts += iter.next()
    }*/

    val allPageViewCounts:ListBuffer[(String,Long)] = ListBuffer()
    val iter = pageViewCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey,entry.getValue))
    }
    //判断定时器
    if(timestamp == ctx.getCurrentKey + 60000L){
      pageViewCountMapState.clear()
      return
    }

    //pageViewCountMapState.clear()

    val sorted = allPageViewCounts.sortWith(_._2 > _._2).take(n)

    //将排名信息格式化为String
    val result = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    //遍历结果列表中的每个ItemViewCount,输出到一行
    for(i <- sorted.indices){  //indices省略0 to ...
      val currentChild = sorted(i)
      result.append("NO").append(i+1).append(":")
        .append("页面url = ").append(currentChild._1).append("\t")
        .append("热门度 = ").append(currentChild._2).append("\n")
    }
    result.append("========================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}