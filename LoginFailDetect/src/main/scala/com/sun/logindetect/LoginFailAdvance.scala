package com.sun.logindetect

import java.net.URL
import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author ：sun
 * @date ：Created in 2020/8/17 18:44
 * @description： TODO
 * @version: 1.0
 */
object LoginFailAdvance {
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
        // 进行判断和检测，如果2秒之内连续登录失败，输出报警信息

        //todo 时间不是顺序的 输出会出现错误
        val loginFailWarningStream: DataStream[LoginFailWarning] = loginEventStream
            .keyBy(_.userId)
            .process(new LoginFailWarningAdvanceResult())
        loginFailWarningStream.print()


        env.execute(getClass.getName)

    }

}

class LoginFailWarningAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {

    lazy val loginFailListState: ListState[LoginEvent]=getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list",classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
        if (value.eventType=="fail"){
           //判断是不是第一次登录失败
            val itr: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
            //如果不是第一次这判断时间差
            if (itr.hasNext){
                val event: LoginEvent = itr.next()
                if (value.timestamp<event.timestamp+2){
                    out.collect(LoginFailWarning(event.userId,event.timestamp,value.timestamp,"login fail 2 times in 2s"))
                }
                loginFailListState.clear()
                loginFailListState.add(value)
            }else{
                loginFailListState.add(value)
            }
        }else{
            loginFailListState.clear()
        }


    }
}
