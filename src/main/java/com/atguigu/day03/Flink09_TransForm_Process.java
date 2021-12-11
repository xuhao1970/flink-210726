package com.atguigu.day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink09_TransForm_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.利用process将单词切分并组成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //4.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //5.使用process实现Sum功能
        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            private ValueState<Integer> valueState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class,0));
//            }

            //保存上一次的累加结果
//            private Integer lastSum = 0;
            private HashMap<String, Integer> map = new HashMap<>();

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //1.当这个单词是第一次来的时候，先存
                if (!map.containsKey(value.f0)){
                    //不存在，则将自己个数存进去
                    map.put(value.f0, value.f1);
                }else {
                    //存在，则取出并重新赋值
                    Integer lastSum = map.get(value.f0);
                    lastSum += value.f1;
                    map.put(value.f0, lastSum);
                }
              /*  Integer lastSum = valueState.value();
                valueState.update(lastSum+value.f1);*/

//                out.collect(Tuple2.of(value.f0,valueState.value()));
                out.collect(Tuple2.of(value.f0,map.get(value.f1)));


            }
        }).print();

        env.execute();
    }
}
