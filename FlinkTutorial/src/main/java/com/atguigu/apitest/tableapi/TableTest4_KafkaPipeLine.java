package com.atguigu.apitest.tableapi;

import javafx.scene.control.Tab;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * create by Shipeixin on  2021-05-20  15:39
 */
public class TableTest4_KafkaPipeLine {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //  连接 kafka读取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect","localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");

        Table sensorTable = tableEnv.from("inputTable");

        //  查询转换
        Table resultStreamTable = sensorTable.select("id,temperature")
                .filter("id = 'sensor_6'");

        Table sqlQuery1 = tableEnv.sqlQuery("select id , temperature from inputTable where id = 'sensor_6'");


        // 建立kafka连接，写出到另一个topic


        /**
         * Exception in thread "main" org.apache.flink.table.api.TableException: AppendStreamTableSink requires that Table has only insert changes.
         * 类似于像聚合这种有更新操作的写入到 kafka，是会报错的，
         * 而像一条一条 没有数据更新的写入 kafka是可以的
         * */
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("Sink")
                .property("zookeeper.connect","localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        /**
                        *  注意写出时，写哪个结果，就要构建相对应的  schema
                        * */
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");


        sqlQuery1.insertInto("inputTable");
    }


}
