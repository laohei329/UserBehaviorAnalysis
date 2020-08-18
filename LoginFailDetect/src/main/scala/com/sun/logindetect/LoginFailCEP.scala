package com.sun.logindetect

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ：sun
 * @date ：Created in 2020/8/17 19:08
 * @description： TODO
 * @version: 1.0
 */
object LoginFailCEP {
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


        //定义匹配模板  比较都是登录失败并且时间间隔为2s的两个序列的比较
        val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
            .begin[LoginEvent]("begin") //新模式序列的开始模式的名称 名字可以自定义 用于识别不同的序列
            .where(_.eventType == "fail")
            .next("next")
            .where(_.eventType == "fail")
            .within(Time.seconds(2))
            //todo 第一个参数是输入的DataStream的数据源  第二个参数是模板
        val patternedStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)
        val loginFailWarningStream: DataStream[LoginFailWarning] = patternedStream.select(new MyPatternSelectFunction())
        loginFailWarningStream.print()

        env.execute(getClass.getName)

    }

}

//PatternSelectFunction<IN, OUT>  设置输入和输出的类型
class MyPatternSelectFunction() extends PatternSelectFunction[LoginEvent,LoginFailWarning]{
    override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
        val firstLoginEcent: LoginEvent = map.get("begin").iterator().next()
        val secondLoginEvent: LoginEvent = map.get("next").iterator().next()
        LoginFailWarning(firstLoginEcent.userId,
            firstLoginEcent.timestamp,
            secondLoginEvent.timestamp,
            "login fail  2 time in "+firstLoginEcent.timestamp +"   to   "+ secondLoginEvent.timestamp)
    }
}
