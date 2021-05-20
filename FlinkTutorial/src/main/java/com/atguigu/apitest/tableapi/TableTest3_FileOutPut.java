package com.atguigu.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/**
 * create by Shipeixin on  2021-05-20  15:21
 */
public class TableTest3_FileOutPut {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        Table sqlAggTable = tableEnv.sqlQuery("select id , count(*)  as CNT, avg(temperature) as avgTemp from inputTable group by id ");

        // 输出到文件
        // 注册输出表
        String outPutPath = "F:\\学习代码\\尚硅谷\\大数据\\Flink\\flink_1.10.1\\FlinkTutorial\\src\\main\\resources\\out.txt";
        tableEnv.connect(new FileSystem().path(outPutPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                ).createTemporaryTable("outPutTable");


        /**
        * Exception in thread "main" org.apache.flink.table.api.TableException: AppendStreamTableSink requires that Table has only insert changes.
         * 类似于像聚合这种有更新操作的写入到文件，是会报错的，
         * 而像一条一条 没有数据更新的写入文件是可以的
        * */
        sqlAggTable.insertInto("outPutTable");
    }
}
