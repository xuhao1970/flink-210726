package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink06_TransForm_Union {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取数据
        DataStreamSource<String> strDStream = env.fromElements("a", "b", "c", "d", "e");

        DataStreamSource<String> intDStream = env.fromElements("1", "2", "3", "4", "5");

        DataStreamSource<String> nameDStream = env.fromElements("张", "王", "李", "赵", "钱");

        //TODO 3.使用Union连接多条流
        DataStream<String> union = strDStream.union(intDStream, nameDStream);

        union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();


        env.execute();
    }
}
