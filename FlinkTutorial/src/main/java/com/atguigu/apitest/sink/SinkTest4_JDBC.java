package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.source.SourceTest4_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * create by Shipeixin on  2021-05-11  17:40
 */
public class SinkTest4_JDBC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取
        DataStream<String> inputStream = env.readTextFile("F:\\学习代码\\尚硅谷\\大数据\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SourceTest4_UDF.MySensorSource());

        // 转换成sensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        sensorReadingDataStreamSource.addSink(new MyJDBCSink());

        env.execute();

    }


    public static class MyJDBCSink extends RichSinkFunction<SensorReading> {

        /**
        *  申明连接 和预编译 语句
        * */
        Connection connection = null;

        PreparedStatement insertStatement = null;

        PreparedStatement updateStatement = null;



        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8","root","root");
            insertStatement = connection.prepareStatement("insert into `fink_sink_jdbc` (id,tmp) values (?,?)");
            updateStatement = connection.prepareStatement("update `fink_sink_jdbc` set tmp = ? where id = ?");
        }


        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 直接更新，如果没更新，那就插入
            updateStatement.setDouble(1,value.getTemperature());
            updateStatement.setString(2,value.getId());

            updateStatement.execute();
            if (updateStatement.getUpdateCount() == 0){
                insertStatement.setString(1,value.getId());
                insertStatement.setDouble(2,value.getTemperature());
                insertStatement.execute();
            }

        }

        @Override
        public void close() throws Exception {
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }

    }

}
