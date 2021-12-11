package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink10_TransForm_Repartition {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("xuhao001", 9999);

        // 3.利用Filter算子将奇数过滤掉
        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {

                return value;
            }
        }).setParallelism(2);

        //TODO 4.keyBy

        KeyedStream<String, String> keyedStream = map.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        //TODO Shuffle
        DataStream<String> shuffle = map.shuffle();

        //TODO rebalance
        DataStream<String> rebalance = map.rebalance();

        //TODO rescale
        DataStream<String> rescale = map.rescale();

        map.print("原始分区").setParallelism(2);
        keyedStream.print("keyBy");
        shuffle.print("shuffle");
        rebalance.print("rebalance");
        rescale.print("rescale");


        env.execute();
    }
}
