package sun.com.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @author ：sun
 * @date ：Created in 2020/8/16 16:11
 * @description： TODO
 * @version: 1.0
 */
object HotItemsWithSql {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val inputStream: DataStream[String] = env.readTextFile("E:\\FinkWordSp\\UserBehaviorAnalysis\\HotitemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

        val dataStream: DataStream[UserBehavior] = inputStream
            .map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        //定义表的执行环境
        val settings: EnvironmentSettings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .useBlinkPlanner()
            .build()

        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
        val dataTable: Table = tableEnv.fromDataStream(dataStream,'itemId, 'behavior, 'timestamp.rowtime as 'ts)
        //todo 对table api进行开窗聚合操作
        val aggTable: Table = dataTable.filter('behavior === "pv")
            .window(Slide over 1.hours every 5.minutes on 'ts as 'ws)
            .groupBy('itemId, 'ws)
            .select('itemId, 'ws.end as 'windowEnd, 'itemId.count as 'cnt)
        //todo 用sql实现topN
        tableEnv.createTemporaryView("aggtable",aggTable)
        val resultTable: Table = tableEnv.sqlQuery(
            """
              |select *
              |from (
              |  select
              |    *,
              |    row_number()
              |      over (partition by windowEnd order by cnt desc)
              |      as row_num
              |    from aggtable )
              |where row_num <= 5
              |""".stripMargin)
        resultTable.toRetractStream[Row].print()
        env.execute("api test")
    }

}
