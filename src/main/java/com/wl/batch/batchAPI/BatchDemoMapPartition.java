package com.wl.batch.batchAPI;


import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class BatchDemoMapPartition {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<String>();

        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);

        MapPartitionOperator<String, String> mapPartitions = text.mapPartition(new MapPartitionFunction<String, String>() {
            public void mapPartition(Iterable<String> values, Collector<String> collector) throws Exception {
                //获取数据库连接 此时是一个分区的数据获取一次连接【有点，每个分区获取一次】
                //values中保存了一个分区的数据
                //处理数据
                //关闭连接
                Iterator<String> iterator = values.iterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();

                    String[] words = next.split("\\W+");
                    for (String word : words) {
                        System.out.println(word);
                    }
                }

            }
        });

        mapPartitions.print();


    }

}
