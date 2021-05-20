package com.atguigu.apitest.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * create by Shipeixin on  2021-05-20  14:11
 */
public class TableTest2_CommonApi {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //  基于老板的 planner 的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()                         //使用老版本的planner
                .inStreamingMode()                       //流处理
                .build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, oldStreamSettings);

        //  基于老版本的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);


//********************************************************************************************************************************//


        // 基于blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()                         //使用新 blink 版本的planner
                .inStreamingMode()                       //流处理
                .build();

        StreamTableEnvironment blinkStreamTbaleEnv = StreamTableEnvironment.create(env, blinkStreamSettings);


        //基于 blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()                         //使用新 blink 版本的planner
                .inBatchMode()                       // 批处理
                .build();

        // StreamTableEnvironment 继承与 TableEnvironment
        TableEnvironment blinkBatchbaleEnv = TableEnvironment.create(blinkBatchSettings);

//&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // 表的创建，连接外部系统读取数据
        String filePath = "F:\\学习代码\\尚硅谷\\大数据\\Flink\\flink_1.10.1\\FlinkTutorial\\src\\main\\resources\\sensor.txt";

        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema().
                        field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                        // 这里填写表的路径，全称为  catalog.database.table    这里简写，只写表名
                ).createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");

        inputTable.printSchema();
        //tableEnv.toAppendStream(inputTable, Row.class).print();



        //  查询zhuanhuan
        Table resultStreamTable = inputTable.select("id,temperature")
                .filter("id = 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count, temperature.avg as avgTemp");

        //SQL
        Table sqlQuery1 = tableEnv.sqlQuery("select id , temperature from inputTable where id = 'sensor_6'");

        Table sqlQuery2 = tableEnv.sqlQuery("select id , count(*)  as CNT, avg(temperature) as avgTemp from inputTable group by id ");


        //打印
        tableEnv.toAppendStream(resultStreamTable, Row.class).print("resultStreamTable");

        // 因为聚合操作是要更新的，因此不能简单的append  而需要retract
        //tableEnv.toRetractStream(aggTable,Row.class).print("aggTable");
        //tableEnv.toRetractStream(sqlQuery1, Row.class).print("sqlQuery1");
        tableEnv.toRetractStream(sqlQuery2, Row.class).print("sqlQuery2");

        env.execute();


    }
}
