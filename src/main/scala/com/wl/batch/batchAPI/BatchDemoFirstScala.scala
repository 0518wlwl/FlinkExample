package com.wl.batch.batchAPI

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoFirstScala {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int,String]]()

    data1.append((1,"zs"))
    data1.append((2,"li"))
    data1.append((3,"wu"))
    data1.append((1,"zk"))
    data1.append((1,"qt"))

    val text1= env.fromCollection(data1)

    //取前三条数据
    text1.first(3).print()

    println("===================================")
    //根据数据中的第一列进行分组，获取每组前2个元素
    text1.groupBy(0).first(2).print()
    println("===================================")
    //根据数据第一列 进行分组 然后在根据第二列进行组内排序 ，获取每组中前2个元素
    text1.groupBy(0).sortGroup(1,Order.ASCENDING).first(2).print()
    println("===================================")
    //不分组 全局排序 获取集合中的前3个元素,针对第一个元素升序，第二个元素降序
    text1.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print()

  }

}
