package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class Flink05_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取数据
        DataStreamSource<String> strDStream = env.fromElements("a", "b", "c", "d", "e");

        DataStreamSource<Integer> intDStream = env.fromElements(1, 2, 3, 4, 5);

        //TODO 3.使用Connect连接两条流
        ConnectedStreams<String, Integer> connect = strDStream.connect(intDStream);

        connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value + "-";
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value + "+";
            }
        }).print();



        env.execute();
    }
}
