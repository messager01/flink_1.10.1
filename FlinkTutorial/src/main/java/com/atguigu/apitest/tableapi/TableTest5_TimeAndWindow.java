package com.atguigu.apitest.tableapi;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * create by Shipeixin on  2021-05-20  16:36
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);


        DataStream<String> inputStream = env.readTextFile("F:\\学习代码\\尚硅谷\\大数据\\Flink\\flink_1.10.1\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                // 单位为 ms   因此需要 * 1000
                return element.getTimestamp() * 1000;
            }
        });


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转换成表，定义时间特性      列名为 pt   proctime 为固定写法，表示时间语义为  process time

       // Table dataTable = tableEnv.fromDataStream(dataStream, "id , timestamp as ts ,temperature as temp , pt.proctime");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id , timestamp as ts ,temperature as temp , rt.rowtime");
        //dataTable.printSchema();

        //tableEnv.toAppendStream(dataTable, Row.class).print();

        tableEnv.createTemporaryView("sensor", dataTable);



        // 5.窗口操作
        //5.1 Group Window

        // Table API
        //  10s钟  事件语义的  滚动窗口
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id,tw")
                // tw.end 可以拿到窗口的结束时间
                .select("id,id.count,temp.avg,tw.end");



        //SQL API
        Table resultSQLTable = tableEnv.sqlQuery("select id,count(id) as cnt, avg(temp) as avgTemp from sensor group by id ,tumble(rt, interval '10' second)");

        tableEnv.toAppendStream(resultTable, Row.class).print();
        //tableEnv.toRetractStream(resultSQLTable, Row.class).print();

        env.execute();
    }
}
