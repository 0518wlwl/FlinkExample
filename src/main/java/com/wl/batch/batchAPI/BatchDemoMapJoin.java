package com.wl.batch.batchAPI;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class BatchDemoMapJoin {

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
        data2.add(new Tuple2<Integer, String>(1,"beojing"));
        data2.add(new Tuple2<Integer, String>(2,"shanghai"));
        data2.add(new Tuple2<Integer, String>(3,"zhoukou"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);



        text1.join(text2).where(0)//指定第一个数据集需要进行比较的元素角标
                .equalTo(0)//指定第二个数据集需要进行比较的元素角标
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3(first.f0,first.f1,second.f1);
                    }
                }).print();


        // map 和with一样
        text1.join(text2).where(0)//指定第一个数据集需要进行比较的元素角标
                .equalTo(0)//指定第二个数据集需要进行比较的元素角标
                .map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple3<Integer,String,String>>() {
                    public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                        return new Tuple3<Integer, String, String>(value.f0.f0,value.f0.f1,value.f1.f1) ;
                    }
                });
    }

}
