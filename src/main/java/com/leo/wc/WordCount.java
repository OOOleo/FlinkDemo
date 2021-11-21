package com.leo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath="D:\\Java\\projects\\FlinkDemo\\src\\main\\java\\com\\leo\\wc\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        //对数据集进行处理  按空格分词展开 转换成（word,1）这样的二元组
        AggregateOperator<Tuple2<String, Integer>> inputSet = inputDataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按空格分词
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).groupBy(0).sum(1);

        inputSet.print();
    }
}
