package com.sun.market_analysis

import java.net.URL
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * @author ：sun
 * @date ：Created in 2020/8/17 11:02
 * @description： TODO
 * @version: 1.0
 */
object AdClickAnalysis {
    def main(args: Array[String]): Unit = {
        //从文件中读取数据
        val source: URL = getClass.getResource("/AdClickLog.csv")
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val dataDS: DataStream[String] = env.readTextFile(source.getPath)

        val logStream: DataStream[AdClickLog] = dataDS.map(data => {
            val arr: Array[String] = data.split(",")
            AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
        }
        ).assignAscendingTimestamps(_.timestamp * 1000L)

        //        val aggStream: DataStream[AdClickCountByProvince] = logStream
        //            .keyBy(_.province)
        //            .timeWindow(Time.minutes(60), Time.seconds(5))
        //            .aggregate(new CountAgg(), new CountResult()) //预聚合  窗口函数
        //

        val filterBlackStream: DataStream[AdClickLog] = logStream
            .keyBy((data => (data.userId, data.adId)))
            .process(new FilterBlackListUser(100))
        val aggStream: DataStream[AdClickCountByProvince] = filterBlackStream
            .keyBy(_.province)
            .timeWindow(Time.hours(1), Time.seconds(5))
            .aggregate(new AdcCountAgg(), new AdcCountFunction())
      //  aggStream.print("aggStream")
        filterBlackStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("warning")
        env.execute(getClass.getName)
    }
}

class AdcCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {

    override def createAccumulator(): Long = 0L

    override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}


//todo 前面keyby 的字段是keyBy(_.province) 这里的key值就是指的province  key的类型就是province的类型
class AdcCountFunction() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {

        val end = new Timestamp(window.getEnd).toString
        //override /*TraversableLike*/ def head: A = iterator.next()
        out.collect(AdClickCountByProvince(end, key, input.head))
    }
}


class FilterBlackListUser(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

    //定义状态 保存用户对广告的点击量，每天0点后清空

    //保存点击量的状态
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
    lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isblack", classOf[Boolean]))

    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
        val count: Long = countState.value() //点击的数量
        // 判断只要是第一个数据来了，直接注册0点的清空状态定时器
        if (count == 0) {
            val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000 //todo  时区的关系要减去8小时
            resetTimerTsState.update(ts)
            ctx.timerService().registerProcessingTimeTimer(ts) //todo 注册时间触发器
        }
        if (count >= maxCount) {
            //点击的次数达到阈值  判断是否已经标记为黑名单
            //todo 然后将黑名单输出到侧输出流
            if (!isBlackState.value()) {
                isBlackState.update(true)
                ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId, value.adId, "Click ad over " + maxCount + " times today."))
            }
            //否则不进行其他操作
            return
        }
        //正常情况下 点击状态更新
        countState.update(count + 1)
        out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {

        if (timestamp == resetTimerTsState.value()) {
            isBlackState.clear()
            countState.clear()
        }
    }


}


//todo 预聚合函数
class CountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

//todo 窗口函数
class CountResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
    //lazy private val dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")

    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
        out.collect(AdClickCountByProvince(formatTs(window.getEnd), key, input.iterator.next()))
    }

    private def formatTs(ts: Long) = {
        val df = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
        df.format(new Date(ts))
    }

}


//定义输入的样例类
case class AdClickLog(userId: Long,
                      adId: Long,
                      province: String,
                      city: String,
                      timestamp: Long)

//定义输出样例类
case class AdClickCountByProvince(windowEnd: String, province: String, cout: Long)

//定义黑名单样例类 侧输出流
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)