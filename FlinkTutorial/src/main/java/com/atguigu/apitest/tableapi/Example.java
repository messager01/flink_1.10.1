package com.atguigu.apitest.tableapi;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * create by Shipeixin on  2021-05-20  13:27
 */
public class Example {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("F:\\学习代码\\尚硅谷\\大数据\\Flink\\flink_1.10.1\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //  创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 基于数据流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);


        // 调用 table Api 操作

        Table resultTable = dataTable.select("id, temperature")
                .where("id = 'sensor_1'");

        // 执行SQL

        tableEnv.createTemporaryView("sensor", dataTable);

        String sql = "select id ,temperature from sensor where id = 'sensor_1'";

        Table resultSQLTable = tableEnv.sqlQuery(sql);


        // 将查询结果输出，使用 tableEnv 中的 toAppendStream，表中的每一行数据都可以看为ROW,所以输出类型可以为row
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toAppendStream(resultSQLTable,Row.class).print("resultSQLTable");


        // 本质上还是流处理，只不过是中间用表的形式表示
        env.execute();


    }
}
