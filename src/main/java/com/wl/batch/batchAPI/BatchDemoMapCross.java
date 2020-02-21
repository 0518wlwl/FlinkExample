package com.wl.batch.batchAPI;


/**
 * 获取笛卡尔积
 */

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;


import java.util.ArrayList;

public class BatchDemoMapCross {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        ArrayList<String> data1 = new ArrayList<String>();

        data1.add("zs");
        data1.add("ww");

        ArrayList<Integer> data2 = new ArrayList<Integer>();
        data2.add(1);
        data2.add(2);

        DataSource<String> text1 = env.fromCollection(data1);
        DataSource<Integer> text2 = env.fromCollection(data2);

        CrossOperator.DefaultCross<String,Integer> cross = text1.cross(text2);

        cross.print();



    }

}
