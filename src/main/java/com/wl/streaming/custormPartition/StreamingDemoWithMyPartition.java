package com.wl.streaming.custormPartition;


import com.wl.streaming.custormSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingDemoWithMyPartition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        //对数据进行转换  把long类型转换为tuble1类型
       DataStream<Tuple1<Long>> tupledata= text.map(new MapFunction<Long, Tuple1<Long>>() {
           public Tuple1<Long> map(Long value) throws Exception {
               return new Tuple1(value);
           }
       });

        DataStream<Tuple1<Long>> partitionDate = tupledata.partitionCustom(new MyPartition(), 0);


        DataStream<Long> result = partitionDate.map(new MapFunction<Tuple1<Long>, Long>() {
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id:" + Thread.currentThread().getId() + ", value" + value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);
        env.execute("own definite partiotn");

    }
}
