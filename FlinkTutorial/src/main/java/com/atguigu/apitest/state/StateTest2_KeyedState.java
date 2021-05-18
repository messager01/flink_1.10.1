package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * create by Shipeixin on  2021-05-18  11:06
 */
public class StateTest2_KeyedState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义有状态的map操作     统计当前sensor数据的个数
        SingleOutputStreamOperator<Integer> res = dataStream.keyBy(SensorReading::getId)
                .map(new MyKeyCountMapper());

        res.print();

        env.execute();

    }

    // 自定义mapfunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading,Integer>{

        /**
        *   此处的 ValueState不能再申明时就初始化，因为申明时任务运行的条件并没有准备好，会报错
         *   因此需要 在 open 方法中，对其进行初始化
        * */
        private ValueState<Integer> keyCountState;


        // 其他类型状态的申明
        private ListState<Integer> myListState;

        private MapState<String,Double> myMapState;

        private ReducingState<SensorReading> myReduceState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 指定ValueSet 的名字 和所存的类型
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));

            //  此处的状态名 不能重名
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("my-list",Integer.class ));

            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));

            //myReduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reduce", new rED, ))
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer count = keyCountState.value();
            if (count == null){
                // 判断是否为初值
                count = 0;
            }
            count++;
            keyCountState.update(count);

            // 各种 state 都有 clear 清空方法
            //keyCountState.clear();
            return count;
        }
    }

}
