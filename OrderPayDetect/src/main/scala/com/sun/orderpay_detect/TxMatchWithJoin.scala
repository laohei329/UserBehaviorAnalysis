package com.sun.orderpay_detect

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author ：sun
 * @date ：Created in 2020/8/18 21:14
 * @description： TODO
 * @version: 1.0
 */
object TxMatchWithJoin {
    def main(args: Array[String]): Unit = {


        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 1. 读取订单事件数据
        val resource1: URL = getClass.getResource("/OrderLog.csv")
        val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource1.getPath)
            //    val orderEventStream = env.socketTextStream("localhost", 7777)
            .map(data => {
                val arr: Array[String] = data.split(",")
                OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .filter(_.eventType == "pay")
            .keyBy(_.payId)

        // 2. 读取到账事件数据
        val resource2: URL = getClass.getResource("/ReceiptLog.csv")
        val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(resource2.getPath)
            //    val orderEventStream = env.socketTextStream("localhost", 7777)
            .map(data => {
                val arr: Array[String] = data.split(",")
                ReceiptEvent(arr(0), arr(1), arr(2).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)
            .keyBy(_.payId)

        val joinedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
            .intervalJoin(receiptEventStream)
            .between(Time.seconds(-20), Time.seconds(50)) //设置下界和上界
            .process(new MyProcessJoinedFunction())
        joinedStream.print("joined result")


        env.execute(getClass.getName)
    }
}

class MyProcessJoinedFunction() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {


    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        out.collect(left,right)
    }


}