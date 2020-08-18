package com.caron

import org.apache.flink.api.common.state.{ListState, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author Caron
 * @create 2020-08-18-14:29
 * @Description ${description}
 * @Version $version
 */
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )

case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

object TxMatch {
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val inputStream = env.readTextFile(resource.getPath)
    val orderEventStream = inputStream.map(
      data=> {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong,arr(1), arr(2),arr(3).toLong)
      }
    )
      //.filter(_.txId != "")
      .filter(_.eventType == "pay")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    val resource1 = getClass.getResource("/ReceiptLog.csv")
    val inputStream1 = env.readTextFile(resource1.getPath)
    val receiptEventStream = inputStream1.map(
      data=> {
        val arr = data.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      }
    )
      .assignAscendingTimestamps(_.eventTime * 1000L)
        .keyBy(_.txId)

    //合并两条流
    val processedStream = orderEventStream
      .connect(receiptEventStream)
      .process(new TxMatchDetection())

    processedStream.print("match")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute()
  }
}

class TxMatchDetection() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  lazy val payEventState : ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay",classOf[OrderEvent]))
  lazy val receiptEventState : ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt",classOf[ReceiptEvent]))
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  override def processElement1(pay: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt = receiptEventState.value()
    if(receipt != null){
      out.collect((pay,receipt))
      receiptEventState.clear()
    }else{
      context.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 1000L * 5)//等待5秒
      payEventState.update(pay)
    }

  }

  override def processElement2(receip: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val pay = payEventState.value()
    if(pay != null){
      out.collect((pay,receip))
      receiptEventState.clear()
      payEventState.clear()
    }else{
      context.timerService().registerEventTimeTimer(receip.eventTime * 1000L + 1000L * 3)//等待3秒
      receiptEventState.update(receip)
    }

  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    if(payEventState.value() != null){
      ctx.output(unmatchedPays,payEventState.value())
    }
    if(receiptEventState.value() != null){
      ctx.output(unmatchedReceipts,receiptEventState.value())
    }
    receiptEventState.clear()
    payEventState.clear()
  }

}