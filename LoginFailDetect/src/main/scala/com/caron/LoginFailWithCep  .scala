package com.caron

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
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

object LoginFailWithCep {
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

    val loginFailPattern = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
        .next("secondFail").where(_.eventType == "fail")
        .within(Time.seconds(2))

    val patternStream = CEP
      .pattern(dataStream.keyBy(_.userId),loginFailPattern)
        .select(new LoginFailEventMatch())
        .print()

    env.execute("Login fail test")
  }
}

class LoginFailEventMatch() extends PatternSelectFunction[LoginEvent,LoginFailWarning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    val firstFail = map.get("firstFail").get(0)
    val secondFail = map.get("secondFail").iterator().next()
    LoginFailWarning( firstFail.userId, firstFail.timestamp, secondFail.timestamp, "login fail" )

  }
}
