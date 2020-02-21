package com.wl.batch.batchAPI;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * broadcast 广播变量
 *
 * 需求:
 * flink会从数据源中获取到用户的姓名
 *
 * 最终需要把用户的姓名和年龄信息打印出来
 *
 * 分析:
 * 所有就需要在中间的map处理的时候获取用户年龄信息
 *
 * 建议把用户的关系数据集使用广播变量进行处理
 *
 * 注意：
 * 如果多个算子要用到同一份数据集 那么需要在对应的多个算子后面分别注册广播变量
 */
public class BatchDemoBroadcast {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1.需要广播的数据
        ArrayList broadData = new ArrayList<Tuple2<String,Integer>>();

        broadData.add(new Tuple2<String,Integer>("zs",15));
        broadData.add(new Tuple2<String,Integer>("ls",12));
        broadData.add(new Tuple2<String,Integer>("zl",13));
        broadData.add(new Tuple2<String,Integer>("tq",15));
        DataSource<Tuple2<String,Integer>> tupleData = env.fromCollection(broadData);

        //1.1.处理需要广播的数据，把数据集转换成map类型，ma中的key就是用户姓名 value是年龄
        MapOperator<Tuple2<String, Integer>, HashMap<String, Integer>> map = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

                HashMap<String, Integer> res = new HashMap<String, Integer>();
                res.put(value.f0, value.f1);
                return res;
            }
        });

        //3.源数据
        DataSource<String> data = env.fromElements("zs", "ls", "zl", "tq");

        //注意 这里要用到 RichMapFunction
        MapOperator<String, String> resut = data.map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            public String map(String value) throws Exception {

                Integer age = allMap.get(value);
                return value + "," + age;
            }

            @Override
            /**
             * 这个方法指挥执行一次
             * 可以在这里实现初始化的功能
             *
             * 所有就可以在open方法中获取广播变量的数据
             */
            public void open(Configuration parameters) throws Exception {
                //获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");

                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }


            }
        }).withBroadcastSet(map, "broadCastMapName");//2. 执行广播数据的操作

        resut.print();

    }

}
