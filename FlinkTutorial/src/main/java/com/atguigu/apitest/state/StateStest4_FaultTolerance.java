package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
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

        // 检查点配置
        // 300ms checkpoint 一次
        env.enableCheckpointing(300);

        // 高级选项
        // 检查点模式选择
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoint 的超时时间，即：该时间内还没完成checkpoint，则为超时
        // 因为checkpoint 时，其他任务不受影响，但是其它任务还是会受影响的
        env.getCheckpointConfig().setCheckpointTimeout(6000L);

        // 允许最大同时进行的checkpoint     默认就为1，表示之只运行一个checkpoint，得等当前checkpoint完了，才能开始下一个checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 两个 checkpoint 最小的间隔时间（保证了两次checkpoint之间要留下一段时间间隔，用来处理数据）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);

        // 容忍 checkpoint 最多失败的次数，默认为0  即 checkpoint 挂掉，任务也会挂掉
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);


        // 重启策略配置
        // 固定延迟重启                                  尝试重启次数   及    重启间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));

        //一定时间内 重启          10分钟内重启3次，每次间隔1分钟，如果都失败，那任务失败
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        env.execute();
    }
}
