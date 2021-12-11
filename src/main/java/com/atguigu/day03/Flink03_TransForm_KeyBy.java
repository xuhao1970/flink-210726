package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        env.setParallelism(4);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("xuhao001", 9999);

        // 3.利用Filter算子将奇数过滤掉
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).setParallelism(2);

        //TODO 4.keyBy
        /*KeyedStream<WaterSensor, String> keyedStream = map.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });*/

        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        SingleOutputStreamOperator<WaterSensor> map1 = keyedStream.map(new RichMapFunction<WaterSensor, WaterSensor>() {
            @Override
            public WaterSensor map(WaterSensor value) throws Exception {
                getRuntimeContext().getJobId();
                return value;
            }

            @Override
            public RuntimeContext getRuntimeContext() {
                return super.getRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        map.print("原始分区").setParallelism(2);
        keyedStream.print("keyBy:");



        env.execute();
    }
}
