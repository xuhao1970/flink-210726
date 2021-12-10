package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.swing.border.EmptyBorder;

public class Flink05_TransForm_Map {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //2.从端口读取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        //3.将读过来的数据转为JavaBean
        /*SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });*/

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MyRichMap());

        map.print();

        env.execute();
    }

    //富函数
    public static class MyRichMap extends RichMapFunction<String,WaterSensor>{
        //生命周期方法
        //在程序最开始的时候调用 每个并行度调用一次
        //通常可以将连接设置在这个方法中
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        //生命周期方法
        //在程序最后的时候调用 每个并行度调用一次  读文件时每个并行度调用两次
        //通常可以将连接的关闭设置在这个方法中
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            System.out.println("-------------------------------------");
            System.out.println(getRuntimeContext().getTaskName());
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }
    }
}
