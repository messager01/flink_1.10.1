package com.atguigu.wc;

/**
 * create by Shipeixin on  2021-05-08  15:04
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
* 批处理 (dataSet) wordcount 处理
* */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境 (批处理执行环境)
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "F:\\学习代码\\尚硅谷\\大数据\\Flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理,按空格分词展开，转换成 （word,1）这样的二元组进行统计
        DataSet<Tuple2<String,Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)     //  将第一个位置的 word 分组
                .sum(1);     //  将第二个位置上的数据求和

        resultSet.print();
    }


    //  自定义类，实现 FlatMapFunction 接口
    //  Tuple2  为 flink 为java 提供的元组类型
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = s.split(" ");
            // 遍历所有word，包成二元组输出
            for (String word : words) {
                // 使用 collector 收集
                out.collect(new Tuple2<>(word,1));
            }
        }
    }

}
