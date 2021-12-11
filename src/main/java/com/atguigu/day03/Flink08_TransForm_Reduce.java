package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_TransForm_Reduce {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口中获取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });


        // 3.将相同id的数据聚和到一块
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);
//        map.keyBy(value -> value.getId())


        //TODO 4.使用聚和算子Reduce
        keyedStream.reduce(new RichReduceFunction<WaterSensor>() {
            /**
             *
             * @param value1 上次聚合后的数据
             * @param value2 当前的数据
             * @return
             * @throws Exception
             */
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//                System.out.println("recude.....");
                return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
            }
        }).print();

        env.execute();
    }
}
