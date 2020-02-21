package com.wl.batch.batchAPI;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class BatchDemoUnion {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<Tuple2<Integer, String>>();

        //用户ID 用户姓名
        data1.add(new Tuple2<Integer, String>(1,"zhangsan"));
        data1.add(new Tuple2<Integer, String>(2,"lisi"));
        data1.add(new Tuple2<Integer, String>(3,"wangwu"));

        ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<Tuple2<Integer, String>>();

        //用户ID 用户所在城市
        data2.add(new Tuple2<Integer, String>(1,"lili"));
        data2.add(new Tuple2<Integer, String>(2,"wanglin"));
        data2.add(new Tuple2<Integer, String>(3,"zhangss"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);


        UnionOperator<Tuple2<Integer, String>> union = text1.union(text2);

        union.print();
    }

}
