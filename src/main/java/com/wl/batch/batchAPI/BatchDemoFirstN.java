package com.wl.batch.batchAPI;


import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class BatchDemoFirstN {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> data = new ArrayList<Tuple2<Integer, String>>();

        //用户ID 用户姓名
        data.add(new Tuple2<Integer, String>(2,"zhangsan"));
        data.add(new Tuple2<Integer, String>(4,"lisi"));
        data.add(new Tuple2<Integer, String>(3,"wangwu"));
        data.add(new Tuple2<Integer, String>(1,"zhaoliu"));
        data.add(new Tuple2<Integer, String>(1,"tianqi"));
        data.add(new Tuple2<Integer, String>(1,"tianqi111"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        //获取前3条数据  按照数据插入的顺序
        text.first(3).print();
        System.out.println("===================");

        //根据数据中的第一列 进行分组 获取每组中的前两个元素
        text.groupBy(0).first(2).print();

        System.out.println("===================================");
        //根据数据第一列 进行分组 然后在根据第二列进行组内排序 ，获取每组中前2个元素

        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();

        System.out.println("===================================");

        //不分组 全局排序 获取集合中的前3个元素

        text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print();

    }

}
