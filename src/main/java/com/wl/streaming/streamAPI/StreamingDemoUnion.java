package com.wl.streaming.streamAPI;

import com.wl.streaming.custormSource.MyParalleSource;
import com.wl.streaming.custormSource.StreamingDemoWithMyNoPralalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingDemoUnion {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<Long> text1 = env.addSource(new MyParalleSource()).setParallelism(1);

        DataStreamSource<Long> text2 = env.addSource(new MyParalleSource()).setParallelism(1);

        DataStream<Long> text = text1.union(text2);

        DataStream<Long> num= text.map(new MapFunction<Long,Long>() {
            public Long map(Long value) throws Exception {

                System.out.println("接收到数据:"+value);
                return value;
            }
        });

        //每两秒钟 处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);

        String jobName = StreamingDemoWithMyNoPralalleSource.class.getSimpleName();

        env.execute("aaa");
    }
}
