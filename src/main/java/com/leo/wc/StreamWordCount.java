package com.leo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        String inputPath="D:\\Java\\projects\\FlinkDemo\\src\\main\\java\\com\\leo\\wc\\hello.txt";

//        DataStreamSource<String> inputDataStream = env.readTextFile(inputPath);

        //从流中读取

        //

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");


//        DataStreamSource<String> inputDataStream = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);
        //
        DataStream<Tuple2<String, Integer>> sum = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
        @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> collector) throws Exception {
                //按空格分词
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }

        }).keyBy(0).sum(1);

        sum.print();

        //执行任务
        env.execute();
    }



}
