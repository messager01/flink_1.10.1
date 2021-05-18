package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * create by Shipeixin on  2021-05-18  14:36
 */
public class StateStest4_FaultTolerance {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());

        env.setStateBackend(new FsStateBackend("hdfs://sfbdp/user/backend"));

        env.setStateBackend(new RocksDBStateBackend("rocksdb url"));



        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        env.execute();
    }
}
