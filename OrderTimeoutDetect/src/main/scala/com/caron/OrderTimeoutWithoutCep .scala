package com.caron

import java.sql.Timestamp

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}

import scala.collection.Map
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author Caron
 * @create 2020-08-17-19:29
 * @Description ${description}
 * @Version $version
 */
object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    val orderTimeOutTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeOut")

    val dataStream = inputStream.map(
      data=> {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong,arr(1),arr(3).toLong)
      }
    )
        .assignAscendingTimestamps(_.eventTime * 1000L)
        .keyBy(_.orderId)
        .process(new OrderPayMatchResult())

    dataStream.print("pay")
    dataStream.getSideOutput(orderTimeOutTag).print("timeout")

    env.execute("pay")
  }
}

class OrderPayMatchResult() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
  // 定义状态，标识位表示create、pay是否已经来过，定时器时间戳
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))
  lazy val isCreateState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("iscreate-state", classOf[Boolean]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts-state", classOf[Long]))
  // 定义侧输出流标签
  val orderTimeOutTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeOut")
  override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()
    val iscreate = isCreateState.value()
    val ts = timerTsState.value()
    // 判断当前事件类型，看是create还是pay
    // 1. 来的是create，要继续判断是否pay过
    if(value.eventType == "create"){
      // 1.1 如果已经支付过，正常支付，输出匹配成功的结果
      if(isPayed){
        collector.collect(OrderResult(value.orderId,"order payed success"))
        // 已经处理完毕，清空状态和定时器
        isCreateState.clear()
        isPayedState.clear()
        timerTsState.clear()
        context.timerService().deleteEventTimeTimer(ts)
      }else{
        // 1.2 如果还没pay过，注册定时器，等待15分钟
        val tsa = value.eventTime * 1000 + 60 * 15 * 1000L
        context.timerService().registerEventTimeTimer(tsa)
        timerTsState.update(tsa)
        isCreateState.update(true)
      }
      // 2. 如果当前来的是pay，要判断是否create过
    }else if(value.eventType == "pay"){
      // 2.1 如果已经create过，匹配成功，还要判断一下pay时间是否超过了定时器时间
      if(iscreate){
        if(value.eventTime * 1000L < ts){
          // 2.1.1 没有超时，正常输出
          collector.collect(OrderResult(value.orderId,"order payed success"))

        }else{
          // 2.1.2 已经超时，输出超时
          context.output(orderTimeOutTag,OrderResult(value.orderId,"order payed success but timeout"))
        }
        // 只要输出结果，当前order处理已经结束，清空状态和定时器
        isCreateState.clear()
        isPayedState.clear()
        timerTsState.clear()
        context.timerService().deleteEventTimeTimer(ts)

      }
      else{
        // 2.2 如果create没来，注册定时器，等到pay的时间就可以
        context.timerService().registerEventTimeTimer(value.eventTime * 1000L)
        //更新状态
        timerTsState.update(value.eventTime * 1000L)
        isPayedState.update(true)
      }

    }//else if
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 定时器触发
    // 1. pay来了，没等到create
    if(isPayedState.value()){
      ctx.output(orderTimeOutTag,OrderResult(ctx.getCurrentKey,"payed but not found create log"))
    }else{
      //2.create来了，没有pay
      ctx.output(orderTimeOutTag,OrderResult(ctx.getCurrentKey,"order timeout"))
    }
    isCreateState.clear()
    isPayedState.clear()
    timerTsState.clear()
  }
}