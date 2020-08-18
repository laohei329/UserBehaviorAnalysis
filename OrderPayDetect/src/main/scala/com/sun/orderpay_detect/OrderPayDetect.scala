package com.sun.orderpay_detect

import java.net.URL

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ：sun
 * @date ：Created in 2020/8/18 18:03
 * @description： TODO
 * @version: 1.0
 */
object OrderPayDetect {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val url: URL = getClass.getResource("/OrderLog.csv")
        val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile(url.getPath).map(
            data => {

                val arr: Array[String] = data.split(",")
                OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            }
        ).assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.orderId)
        //todo 1.定义pattern
        val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
            .begin[OrderEvent]("create").where(_.eventType == "create")
            .followedBy("pay").where(_.eventType == "pay")
            .within(Time.minutes(15))
        //todo 2.数据流中引入pattern模组

        val patternedStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, pattern)

        //定义侧输出流
        val outputtage = new OutputTag[OrderResult]("warning")
        val orderResultStream: DataStream[OrderResult] = patternedStream.select(outputtage) {
            (map: collection.Map[String, Iterable[OrderEvent]], time: Long) =>
                val orderEvent: OrderEvent = map.get("create").get.iterator.next()
                OrderResult(orderEvent.orderId, "timeout")

        } {
            (map: collection.Map[String, Iterable[OrderEvent]]) =>
                val orderEvent: OrderEvent = map.get("pay").get.iterator.next()
                OrderResult(orderEvent.orderId, "payed successfully")
        }
        orderResultStream.print("payed")
        orderResultStream.getSideOutput(outputtage).print("warning")

        env.execute(getClass.getName)
    }

}


//定义输入数据样例类
case class OrderEvent(orderId: Long, eventType: String, payId: String, eventTime: Long)

//定义输出样例类
case class OrderResult(orderId: Long, eventType: String)




