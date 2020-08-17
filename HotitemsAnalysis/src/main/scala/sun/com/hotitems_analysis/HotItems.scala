package sun.com.hotitems_analysis

import java.util
import java.util.Properties

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author ：sun
 * @date ：Created in 2020/8/14 14:34
 * @description： TODO
 * @version: 1.0
 */
object HotItems {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //   val inputPath = "E:\\FinkWordSp\\UserBehaviorAnalysis\\HotitemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
        //  val baseDS: DataStream[String] = env.readTextFile(inputPath)
        val topic: String = "hotitems"
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop202:9092")
        properties.setProperty("group.id", "hotitems-group")

        //todo 从kafka中读取书
        val baseDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties))

        val userBehaviorDS: DataStream[UserBehavior] = baseDS.map { data =>
            val arr: Array[String] = data.split(",")
            UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        }.assignAscendingTimestamps(_.timestamp * 1000L) //todo 如果已经排好序这是只用这个
        // .assignTimestampsAndWatermarks()  如果没有排序这是使用这个

        //得到窗口聚合的结果
        val aggStream: DataStream[ItemViewCount] = userBehaviorDS
            .filter(_.behavior == "pv")
            .keyBy("itemId")
            .timeWindow(Time.minutes(60), Time.minutes(5)) //设置窗口的大小和滑动的长度
            .aggregate(new CountAgg(), new ItemViewWindowResult()) //第一个参数是用于预聚合和函数  第二个是窗口函数

        val topNStream: DataStream[String] = aggStream.keyBy("windowEnd") //按照窗口，收集当前窗口内的商品count数据
            .process(new TopNHotItems(5))
        topNStream.print()


        env.execute("hotitems")
    }

}


// 自定义KeyedProcessFunction
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {


    //先定义ListState
    private var itemViewCountListState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
        itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
        itemViewCountListState.add(value)
        //注册定时器  windowEnd + 1 之后触发的定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    //当定时器触发  可以认为 所有窗口统计结果已到齐
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        val allItemViewCount: ListBuffer[ItemViewCount] = ListBuffer()
        val iter: util.Iterator[ItemViewCount] = itemViewCountListState.get().iterator()
        while (iter.hasNext) {
            allItemViewCount.append(iter.next())
            // allItemViewCount += iter.next()
        }
        //清空状态
        itemViewCountListState.clear()
        //按照count的大小排序 取前n个
        val sortedItemViewCount: ListBuffer[ItemViewCount] = allItemViewCount.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
        //将排名信息格式化成String  便于输出打印可视化展示
        val result = new StringBuilder
        result.append("窗口结束时间：").append((new Timestamp(timestamp - 1))).append("\n")

        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for (i <- sortedItemViewCount.indices) { //indices 得到的是索引
            val itemViewCount: ItemViewCount = sortedItemViewCount(i)
            result.append("NO").append(i + 1).append(": \t")
                .append("商品ID = ").append(itemViewCount.itemId).append("\t")
                .append("热门度 = ").append(itemViewCount.count).append("\n")
        }
        result.append("==================================\n\n")

        Thread.sleep(1000)
        out.collect(result.toString())

    }
}

//预聚合的函数 TODO 输入类型是UserBehavior  中间是聚合状态  最后还是输出的类型
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

    override def createAccumulator(): Long = 0L

    //每来一次数据 调用一次a得到 ，count值加一
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    //Merges two accumulators, returning an accumulator with the merged state
    //两个accumulator 聚合的方式
    override def merge(a: Long, b: Long): Long = a + b
}

//todo 定义一个求平均值的预聚合函数
//中间预聚合的状态是  时间戳的综合和数据的个数
class AvgAgg() extends AggregateFunction[UserBehavior, (Long, Int), Long] {
    override def createAccumulator(): (Long, Int) = (0L, 0)

    override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

    override def getResult(accumulator: (Long, Int)): Long = accumulator._1 / accumulator._2

    override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, b._2 + b._2)
}

//自定义窗口函数windowFunction  TODO 窗口函数的输入类型是预聚合函数的输出类型
class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    /**
     * 计算窗口并输出一个或多个元素。
     *
     * @param key    此窗口要计算的键。
     * @param window 正在被评估的窗口。
     * @param input  正在计算的窗口中的元素。
     * @param out    用于发射元素的收集器。
     */
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        val itemId = key.asInstanceOf[Tuple1[Long]].f0
        val windowEnd: Long = window.getEnd //Gets the end timestamp of this window.
        val count: Long = input.iterator.next()
        out.collect(ItemViewCount(itemId, windowEnd, count))
    }
}

//662867,2244074,1575622,pv,1511658000
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
