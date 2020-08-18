package com.sun.networkflow_analysis

import java.net.URL
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author ：sun
 * @date ：Created in 2020/8/17 19:54
 * @description： TODO
 * @version: 1.0
 */
object HotPagesNetworkFlow {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //数据源准备
        val url: URL = getClass.getResource("/apache.log")
        val dataStream: DataStream[String] = env.readTextFile(url.getPath)
        val apacheLogEventStream: DataStream[ApacheLogEvent] = dataStream.map(data => {
            //17/05/2015:10:05:19
            val format = new SimpleDateFormat("dd/MM/YYYY:HH:mm:ss")
            val arr: Array[String] = data.split(" ")
            val ts: Long = format.parse(arr(3)).getTime
            ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
        }
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
            override def extractTimestamp(element: ApacheLogEvent) = element.timestamp
        })
        val aggStream: DataStream[PageViewCount] = apacheLogEventStream
            .filter(_.method == "GET")
            .keyBy(_.url)
            .timeWindow(Time.minutes(10), Time.seconds(5))
            .aggregate(new PageCountAgg(), new PageWindowResult())


        val topNStream: DataStream[String] = aggStream
            .keyBy(_.windowEnd)
            .process(new TopNProcessFunction(3))
        topNStream.print()


        env.execute(getClass.getName)
    }

}

class TopNProcessFunction(topN: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
    lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))


    override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
        pageViewCountMapState.put(value.url, value.count)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        // 另外注册一个定时器，1分钟之后触发，这时窗口已经彻底关闭，不再有聚合结果输出，可以清空状态
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

        // 判断定时器触发时间，如果已经是窗口结束时间1分钟之后，那么直接清空状态
        if (timestamp == ctx.getCurrentKey + 60000L) {
            pageViewCountMapState.clear()
            return
        }

        val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
        val iter = pageViewCountMapState.entries().iterator()
        while (iter.hasNext) {
            val entry = iter.next()
            allPageViewCounts += ((entry.getKey, entry.getValue))
        }
        // 按照访问量排序并输出top n
        // val sortedPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(topN)
        val sortedPageViewCounts = allPageViewCounts.sortBy(_._2)(Ordering.Long.reverse).take(topN)
        // 将排名信息格式化成String，便于打印输出可视化展示
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for (i <- sortedPageViewCounts.indices) {
            val currentItemViewCount = sortedPageViewCounts(i)
            result.append("NO").append(i + 1).append(": \t")
                .append("页面URL = ").append(currentItemViewCount._1).append("\t")
                .append("热门度 = ").append(currentItemViewCount._2).append("\n")
        }

        result.append("\n==================================\n\n")

        Thread.sleep(1000)
        out.collect(result.toString())


    }


}

/*
class TopNProcessFunction(topN: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
    lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext
        .getMapState(new MapStateDescriptor[String, Long]("pageViewMap", classOf[String], classOf[Long]))

    override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[String, PageViewCount, String]#Context, out: Collector[String]): Unit = {
        pageViewCountMapState.put(value.url, value.count)
        ctx.timerService().registerEventTimeTimer((value.windowEnd.toLong + 1))
        ctx.timerService().registerEventTimeTimer((value.windowEnd.toLong + 6000L))
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

        if (timestamp == ctx.getCurrentKey + 6000L) {
            pageViewCountMapState.clear()
            return
        }
        val allPageViewCount: ListBuffer[(String, Long)] = ListBuffer()
        val itr: util.Iterator[Map.Entry[String, Long]] = pageViewCountMapState.entries().iterator()
        if (itr.hasNext) {
            val entry: Map.Entry[String, Long] = itr.next()
            allPageViewCount.append((entry.getKey, entry.getValue))
        }
        val sortedPageViewCounts: ListBuffer[(String, Long)] = allPageViewCount.sortBy(_._2)(Ordering.Long.reverse).take(topN)


        // 将排名信息格式化成String，便于打印输出可视化展示
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for (i <- sortedPageViewCounts.indices) {
            val currentItemViewCount = sortedPageViewCounts(i)
            result.append("NO").append(i + 1).append(": \t")
                .append("页面URL = ").append(currentItemViewCount._1).append("\t")
                .append("热门度 = ").append(currentItemViewCount._2).append("\n")
        }

        result.append("\n==================================\n\n")
        out.collect(result.toString())

    }

}
*/


class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}


class PageWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {

    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
        out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
    }

}

//83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, count: Long)