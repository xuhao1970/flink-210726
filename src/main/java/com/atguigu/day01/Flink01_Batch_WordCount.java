package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件中的数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //先flatMap（按照空格切分，然后打散成Tuple2元组（word，1））->reduceByKey（将相同单词的数据聚和到一块并做累加）->打印到控制台
        //3.将数据按照空格切分，然后组成Tuple2元组
//        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = dataSource.flatMap(new MyFlatMap());

        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = dataSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            //将数据按照空格切分
            String[] words = s.split(" ");
            //遍历出每一个单词
            for (String word : words) {
                //将单词组成Tuple2元组通过采集器发送至下游
//                collector.collect(new Tuple2<>(word, 1));
                collector.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT));

        //4.将相同的单词聚合到一块
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //5.将单词的个数做累加
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //6.打印到控制台
        result.print();
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{

        /**
         *
         * @param s 输入的数据
         * @param collector 采集器，将数据采集起来发送至下游
         * @throws Exception
         */
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //将数据按照空格切分
            String[] words = s.split(" ");
            //遍历出每一个单词
            for (String word : words) {
                //将单词组成Tuple2元组通过采集器发送至下游
//                collector.collect(new Tuple2<>(word, 1));
                collector.collect(Tuple2.of(word,1));
            }
        }
    }
}
