package com.wl.batch.batchAPI;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * 外连接
 *
 * 左外连接
 * 右外连接
 * 全连接
 */
public class BatchDemoMapOuterJoin {

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
        data2.add(new Tuple2<Integer, String>(4,"zhoukou"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);


        /**
         * 左外连接
         *
         * 注意:second中的元素可能为null
         */
        text1.leftOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                       if (second==null){
                           return new Tuple3<Integer, String, String>(first.f0,first.f1,"null");
                       }else {
                           return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                       }


                    }
                }).print();

        System.out.println("=============================================================");

        /**
         * 右外连接
         *
         * 注意： first中的数据可能为null
         */

        text1.rightOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                       if (first==null){
                           return new Tuple3<Integer, String, String>(second.f0,"null",second.f1);
                       }else {
                           return new Tuple3<Integer, String, String>(second.f0,first.f1,second.f1);
                       }
                    }
                }).print();

        System.out.println("=============================================================");

        /**
         * 全外连接
         *
         * 注意:first和 second中的数据都可能为空
         */

        text1.fullOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first==null){
                            return new Tuple3<Integer, String, String>(second.f0,"null",second.f1);
                        }else if(second==null){
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,"null");
                        }else {
                            return new Tuple3<Integer, String, String>(second.f0,first.f1,second.f1);
                        }
                    }
                }).print();

    }

}
