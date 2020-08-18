package com.caron

import java.sql.Timestamp

import scala.collection.Map
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Caron
 * @create 2020-08-17-19:29
 * @Description ${description}
 * @Version $version
 */
case class OrderEvent(orderId:Long,eventType:String,eventTime:Long)
case class OrderResult(orderId:Long,eventType:String)
object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    val dataStream = inputStream.map(
      data=> {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong,arr(1),arr(3).toLong)
      }
    )
        .assignAscendingTimestamps(_.eventTime * 1000L)

    //
    val pattern1 = Pattern
      .begin[OrderEvent]("create")
        .where(_.eventType == "create")
        .followedBy("pay")
        .where(_.eventType == "pay")
        .within(Time.minutes(15))

    val patternStream = CEP.pattern(dataStream.keyBy(_.orderId),pattern1)

    //定义一个输出标签，用来标记侧输出流
    val orderTimeOutTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeOut")

    val result: DataStream[OrderResult] = patternStream.select(orderTimeOutTag)(
      //定义一个超时的函数
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        //获取到超时的订单id
        val timeOutOrderId = pattern.getOrElse("create", null).iterator.next().orderId
        val ts = new Timestamp(timestamp).toString
        val time = ts.substring(0,ts.indexOf("."))
        OrderResult(timeOutOrderId, time  + " order time out" )
      }
    )(
      //定义一个查询结果的函数
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val payOrderId = pattern.getOrElse("pay", null).iterator.next().orderId
        OrderResult(payOrderId, "order payed success")
      }
    )

    result.print("pay")
    result.getSideOutput(orderTimeOutTag).print("timeout")

    env.execute("pay")
  }
}
