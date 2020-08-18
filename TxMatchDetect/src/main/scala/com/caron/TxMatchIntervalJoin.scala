package com.caron

import org.apache.flink.api.common.state.{ListState, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author Caron
 * @create 2020-08-18-14:29
 * @Description ${description}
 * @Version $version
 */
object TxMatchIntervalJoin {
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
      .intervalJoin(receiptEventStream)
      .between(Time.seconds(-15), Time.seconds(5))
      .process(new TxMatchIntervalJoin())

    processedStream.print("match")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute()
  }
}

class TxMatchIntervalJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(in1: OrderEvent, in2: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect( (in1, in2) )
  }
}