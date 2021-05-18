package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.commons.math3.FieldElement;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * create by Shipeixin on  2021-05-18  10:46
 */
public class StateTest1_OperatorState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream(null, 0);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义有状态的map操作     统计当前分区的数据个数

        SingleOutputStreamOperator<Integer> res = dataStream.map(new MyCountMap());

        res.print();

        env.execute();

    }


    public static class MyCountMap implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer> {

        //  定义一个本地变量，作为算在状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }

        //  对状态做快照
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }


        // 发生故障时通过该方法来恢复状态
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer integer : state) {
                count += integer;
            }
        }
    }

}
