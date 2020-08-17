package com.sun.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @author ：sun
 * @date ：Created in 2020/8/17 10:12
 * @description： TODO
 * @version: 1.0
 */
object AppMarketingByChannel {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulateSource)
            .assignAscendingTimestamps(_.timestamp)
        dataStream
            .filter(_.behavior != "uninstall")
            .keyBy(makeuser => (makeuser.channel, makeuser.behavior))
            .timeWindow(Time.days(1), Time.seconds(5))
            //.aggregate()//todo 预聚合函数  窗口函数
            .process(new MarketCountByChannel()).print() //全窗口函数

        env.execute("appmark")
    }

}

class MarketCountByChannel() extends ProcessWindowFunction[MarketingUserBehavior, MarketViewCount, (String, String), TimeWindow] {


    override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketViewCount]): Unit = {
        val start = new Timestamp(context.window.getStart).toString
        val end = new Timestamp(context.window.getEnd).toString
        val channel = key._1
        val behavior = key._2
        val count: Int = elements.size
        out.collect(MarketViewCount(start,end,channel,behavior))

    }

}

//TODO 定义输出窗口的样例类
case class MarketViewCount(windowStart: String,
                           windowEnd: String,
                           channel: String,
                           behavior: String)

//todo  定义输入数据的样例类
case class MarketingUserBehavior(userId: String,
                                 behavior: String,
                                 channel: String, //渠道
                                 timestamp: Long)

//TODO 自定义测试数据源
class SimulateSource() extends RichSourceFunction[MarketingUserBehavior] {
    //设置运行标识
    var running = true
    //定义用户行为聚到
    val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
    val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
    val rand: Random = Random

    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
        //todo 定义一个生成数据最大的数量
        val maxCounts = Long.MaxValue
        var count = 0L
        while (running && count < maxCounts) {
            val id = UUID.randomUUID().toString
            //todo 在预设的集合里面随机拿数据
            //Random.nextint  从这个随机数生成器的序列中返回一个伪随机的、均匀分布的int值，该值在0(包含)和指定的值(不包含)之间。
            val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
            val channel = channelSet(rand.nextInt(channelSet.size))
            val ts = System.currentTimeMillis()
            ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))
            count += 1
            Thread.sleep(300)
        }
    }

    override def cancel(): Unit = running = false

}
