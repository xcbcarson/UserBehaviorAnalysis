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

object LoginFailAdvance  {
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
      .process(new LoginFailWarningAdvanceResult())
      .print()

    env.execute("Login fail test")
  }
}

class  LoginFailWarningAdvanceResult() extends KeyedProcessFunction[Long,LoginEvent,LoginFailWarning]{
  lazy val loginFailListState :ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, collector: Collector[LoginFailWarning]): Unit = {
    // 首先判断事件类型
    if( value.eventType == "fail" ){
      // 1. 如果是失败，进一步做判断
      val iter = loginFailListState.get().iterator()
      // 判断之前是否有登录失败事件
      if(iter.hasNext){
        // 1.1 如果有，那么判断两次失败的时间差
        val firstFailEvent = iter.next()
        if( value.timestamp < firstFailEvent.timestamp + 2 ){
          // 如果在2秒之内，输出报警
          collector.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
        }
        // 不管报不报警，当前都已处理完毕，将状态更新为最近依次登录失败的事件
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        // 1.2 如果没有，直接把当前事件添加到ListState中
        loginFailListState.add(value)
      }
    } else {
      // 2. 如果是成功，直接清空状态
      loginFailListState.clear()
    }

  }
}