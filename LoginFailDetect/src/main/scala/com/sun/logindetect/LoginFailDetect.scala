package com.sun.logindetect

import java.net.URL
import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author ：sun
 * @date ：Created in 2020/8/17 14:15
 * @description： TODO
 * @version: 1.0
 */
object LoginFailDetect {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val resource: URL = getClass.getResource("/LoginLog.csv")
        val dataStream: DataStream[String] = env.readTextFile(resource.getPath)

        val loginEventStream: DataStream[LoginEvent] = dataStream.map(data => {

            val arr: Array[String] = data.split(",")
            LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
        }
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
            override def extractTimestamp(element: LoginEvent) = element.timestamp * 1000L
        })
        val loginFailWarningStrean: DataStream[LoginFailWarning] = loginEventStream
            .keyBy(_.userId)
            .process(new LoginFailWarningResult(2))
        loginFailWarningStrean.print()


        env.execute(getClass.getName)
    }
}

class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
    //定义状态 把登录失败的时间保存，保存定时器的时间

    lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))
    lazy val timeStampState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timestamp", classOf[Long]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
        if (value.eventType == "fail") {
            loginFailListState.add(value)
            //如果没有定时器注册定时器，并且出发时间为2s
            if (timeStampState.value() == 0L) {
                val ts = value.timestamp * 1000L + 2000L
                timeStampState.update(ts)
                ctx.timerService().registerEventTimeTimer(ts)
            }
        } else {
            //如果是登录成功  则清空状态的记录
            ctx.timerService().deleteEventTimeTimer(timeStampState.value())
            timeStampState.clear()
            loginFailListState.clear()

        }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
        val allLoginFailList: ListBuffer[LoginEvent] = ListBuffer()
        //定时器触发
        val itrs: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
        while (itrs.hasNext) {
            val loginEvent: LoginEvent = itrs.next()
            allLoginFailList.append(loginEvent)
        }
        if (allLoginFailList.length > failTimes) {

            out.collect(LoginFailWarning(allLoginFailList.head.userId, allLoginFailList.head.timestamp, allLoginFailList.last.timestamp, "login fail in 2s for " + allLoginFailList.length + " times."))
        }
        loginFailListState.clear()
        timeStampState.clear()

    }

}

//定义输入样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

//定义输出样例类
case class LoginFailWarning(userId: Long, firstTime: Long, lastTime: Long, warningMsg: String)
