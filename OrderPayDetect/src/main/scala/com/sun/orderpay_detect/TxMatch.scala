package com.sun.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：sun
 * @date ：Created in 2020/8/18 20:38
 * @description： TODO
 * @version: 1.0
 */
object TxMatch {

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

        val matchedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
            .connect(receiptEventStream)
            .process(new TxMatchProcessFunction())


        matchedStream.print("result")
        matchedStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pay")).print("unmatched-pay")
        matchedStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched-receipt")

        env.execute(getClass.getName)

    }

}

class TxMatchProcessFunction() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    //定义类的状态
    lazy val orderEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("orderEventState", classOf[OrderEvent]))
    lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptEventState", classOf[ReceiptEvent]))


    val unmatchedPayEventOutputTag = new OutputTag[OrderEvent]("unmatched-pay")
    val unmatchedReceiptEventOutputTag = new OutputTag[ReceiptEvent]("unmatched-receipt")

    override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        val receiptEvent: ReceiptEvent = receiptEventState.value()
        //如果receiptEventState 已经存过ReceiptEvent的状态  证明这个地方的数据是正常的  可以正常的输出
        if (receiptEvent != null) {
            out.collect(value, receiptEvent)
            receiptEventState.clear()
            orderEventState.clear()
        } else {
            //证明ReceiptEvent数据还没来  注册定时器 并更新状态
            ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
            orderEventState.update(value)
        }
    }

    override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        val orderEvent: OrderEvent = orderEventState.value()
        if (orderEvent != null) {
            out.collect(orderEvent, value)
            orderEventState.clear()
            receiptEventState.clear()

        } else {
            ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 3000L)
            receiptEventState.update(value)
        }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        //前面没有删除定时器所以 这个地方设置的所有定时器都会触发  我们只要找到定时器触发的时候哪些状态还是存在的进行判断就可以了
        val orderEvent: OrderEvent = orderEventState.value()
        //当orderEventState没有清空的时候证明 ReceiptEvent 数据没有来‘

        if (orderEvent != null) {
            ctx.output(unmatchedPayEventOutputTag, orderEvent)
        }

        val receiptEvent: ReceiptEvent = receiptEventState.value()
        if (receiptEvent != null) {
            ctx.output(unmatchedReceiptEventOutputTag, receiptEvent)
        }

        orderEventState.clear()
        receiptEventState.clear()

    }

}


case class ReceiptEvent(payId: String, payType: String, timestamp: Long)
