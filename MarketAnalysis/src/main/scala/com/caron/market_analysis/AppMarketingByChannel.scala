package com.caron.market_analysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @author Caron
 * @create 2020-08-17-10:13
 * @Description ${description}
 * @Version $version
 */

case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
//输入数据源生产
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {
  //是否运行标示位
  var running = true
  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val rand: Random = Random

  override def cancel(): Unit = {
    running = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      sourceContext.collectWithTimestamp(MarketingUserBehavior(id, behaviorType, channel, ts), ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }
  }
}
  //输出数据样例类
  case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

  object AppMarketingByChannel {
    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val dataStream = env.addSource(new SimulatedEventSource)
        .assignAscendingTimestamps(_.timestamp)

      val resultStream = dataStream
        .filter(_.behavior != "UNINSTALL")
        .keyBy(rdd => {
          (rdd.channel, rdd.behavior)
        })
        .timeWindow(Time.days(1), Time.seconds(5))
        //.aggregate(new )
        .process(new MarketCountByChannel())
        .print()

      env.execute("Market")
    }
  }

class MarketCountByChannel() extends ProcessWindowFunction[MarketingUserBehavior,MarketViewCount,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val count = elements.size
    out.collect(MarketViewCount(start,end,key._1,key._2,count))
  }
}