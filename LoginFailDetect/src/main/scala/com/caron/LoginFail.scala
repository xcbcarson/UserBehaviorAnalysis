package com.caron

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author Caron
 * @create 2020-08-17-14:16
 * @Description ${description}
 * @Version $version
 */
case class LoginEvent(userId :Long,ip:String,eventType: String,timestamp: Long)
// 输出报警信息样例类
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, waringMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val filepath = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(filepath.getPath)

    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
      override def extractTimestamp(t: LoginEvent): Long = {
        t.timestamp * 1000L
      }
    })

    val loginWarning = dataStream
      .keyBy(_.userId)
      .process(new LoginFailWarningResult(2))
      .print()

    env.execute("Login fail test")
  }
}

class  LoginFailWarningResult(failTimes:Int) extends KeyedProcessFunction[Long,LoginEvent,LoginFailWarning]{
  lazy val loginFailListState :ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list",classOf[LoginEvent]))
  lazy val timerState : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts",classOf[Long]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, collector: Collector[LoginFailWarning]): Unit = {
    if(i.eventType == "fail"){
      loginFailListState.add(i)
      // 如果没有定时器，那么注册一个2秒后的定时器
      if(timerState.value() == 0){
        val ts = i.timestamp* 1000L + 2000L
        context.timerService().registerEventTimeTimer(ts)
        timerState.update(ts)
      }

    }else{
      // 如果是成功，那么直接清空状态和定时器，重新开始
      context.timerService().deleteEventTimeTimer(timerState.value())
      loginFailListState.clear()
      timerState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val allLoginFailList:ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailListState.get().iterator()
    while(iter.hasNext){
      allLoginFailList += iter.next()
    }
    // 判断登录失败事件的个数，如果超过了上限，报警
    if(allLoginFailList.length >= failTimes){
      out.collect(
        LoginFailWarning(
          allLoginFailList.head.userId,
          allLoginFailList.head.timestamp,
          allLoginFailList.last.timestamp,
          "login fail in 2s for " + allLoginFailList.length + " times."
        )
      )
    }
    // 清空状态
    loginFailListState.clear()
    timerState.clear()
  }
}