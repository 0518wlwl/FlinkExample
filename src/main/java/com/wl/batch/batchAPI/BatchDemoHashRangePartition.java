package com.wl.batch.batchAPI;
/**
 *  Hash-Partition
 *
 *  Range-Partition
 */

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class BatchDemoHashRangePartition {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> data = new ArrayList<Tuple2<Integer, String>>();

        //用户ID 用户姓名
        data.add(new Tuple2<Integer, String>(1,"hello1"));
        data.add(new Tuple2<Integer, String>(1,"hello2"));
        data.add(new Tuple2<Integer, String>(2,"hello3"));
        data.add(new Tuple2<Integer, String>(2,"hello4"));
        data.add(new Tuple2<Integer, String>(3,"hello5"));
        data.add(new Tuple2<Integer, String>(3,"hello6"));
        data.add(new Tuple2<Integer, String>(3,"hello7"));
        data.add(new Tuple2<Integer, String>(4,"hello8"));
        data.add(new Tuple2<Integer, String>(4,"hello9"));
        data.add(new Tuple2<Integer, String>(5,"hello10"));
        data.add(new Tuple2<Integer, String>(5,"hello11"));
        data.add(new Tuple2<Integer, String>(6,"hello12"));
        data.add(new Tuple2<Integer, String>(6,"hello13"));
        data.add(new Tuple2<Integer, String>(6,"hello14"));
        data.add(new Tuple2<Integer, String>(6,"hello15"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);
        
        text.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, String>() {
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<String> out) throws Exception {

                Iterator<Tuple2<Integer, String>> iterator = values.iterator();
                while(iterator.hasNext()){
                    Tuple2<Integer, String> next = iterator.next();
                    System.out.println("当前线程ID:"+Thread.currentThread().getId()+","+next);

                }

            }
        }).print();


    }

}
