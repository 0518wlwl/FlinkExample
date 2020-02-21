package com.wl.batch.batchAPI;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class BatchDemoMapDisinct {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<String>();

        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);

        FlatMapOperator<String, String> flatMapData = text.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.toLowerCase().split("\\W+");
                for (String word : split) {
                    out.collect(word);
                }

            }
        });

        flatMapData.distinct().print();




    }

}
