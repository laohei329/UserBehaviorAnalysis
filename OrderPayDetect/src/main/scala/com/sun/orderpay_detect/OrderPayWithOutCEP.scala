package com.sun.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：sun
 * @date ：Created in 2020/8/18 18:42
 * @description： TODO
 * @version: 1.0
 */
object OrderPayWithOutCEP {
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

        val orderResultStream: DataStream[OrderResult] = orderEventStream.process(new MyKeyedProcessFunction())
        orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")
        orderResultStream.print("result")

        env.execute(getClass.getName)
    }

}

//利用状态编程  实现create 和 pay  之间15分钟内是操作
class MyKeyedProcessFunction() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    lazy val isCreateState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCreate-state", classOf[Boolean]))
    lazy val isPayState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispay-state", classOf[Boolean]))
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-state", classOf[Long]))
    // 定义侧输出流标签
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
        val isCreate: Boolean = isCreateState.value()
        val isPay: Boolean = isPayState.value()
        val ts: Long = timerState.value()

        //todo 1.判断当前事件是create 还是 pay
        if (value.eventType == "create") {
            //判断是否已经支付过
            if (isPay) {
                out.collect(OrderResult(value.orderId, "payed successfully"))
                isCreateState.clear()
                isPayState.clear()
                timerState.clear()
                ctx.timerService().deleteEventTimeTimer(ts)
            } else {
                //r如果没有支付  等待15分钟  注册定时器
                val time: Long = value.eventTime * 1000L + 900 * 1000L
                ctx.timerService().registerEventTimeTimer(time)
                isCreateState.update(true)
                timerState.update(time)
            }
        } else {
            if (isCreate) {
                //判断是否超过定时器的时间

                if (value.eventTime * 1000L < ts) {
                    //没有超时
                    out.collect(OrderResult(value.orderId, "success"))
                } else {
                    ctx.output(orderTimeoutOutputTag,OrderResult(value.orderId, "payed but already timeout"))
                }

                isCreateState.clear()
                isPayState.clear()
                timerState.clear()
                ctx.timerService().deleteEventTimeTimer(ts)
            } else {
                // 2.2 如果create没来，注册定时器，等到pay的时间就可以
                ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
                timerState.update(value.eventTime * 1000L)
                isPayState.update(true)
            }

        }


    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
        // 定时器触发
        // 1. pay来了，没等到create
        if (isPayState.value()) {
            ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but not found create log"))
        } else {
            // 2. create来了，没有pay
            ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
        }
        // 清空状态


        // 清空状态
        isCreateState.clear()
        isPayState.clear()
        timerState.clear()
    }


}
