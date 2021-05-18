package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.lang.reflect.Executable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * create by Shipeixin on  2021-05-11  11:32
 */
public class SourceTest4_UDF {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();

        env.execute();

    }


    /**
    *  实现自定义的  SourceFunction
    * */

    public static class MySensorSource implements SourceFunction<SensorReading>{

        // 定义标志位，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {

            // 定义随机数发生器
            Random random = new Random();

            // 设置10个传感器的初始温度值
            Map<String,Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++){
                sensorTempMap.put("sensor" + (i+1),60 + random.nextGaussian() * 20);
            }

            while (running){
                for (String sensorId : sensorTempMap.keySet()) {
                    // 在当前温度基础上随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    sourceContext.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
                }

                //  控制输出频率
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
