package com.wl.batch.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoUnionScala {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
   val data1 =  ListBuffer[Tuple2[Int,String]]()

    data1.append((1,"zs"))
    data1.append((2,"ls"))
    data1.append((3,"ww"))

    val data2 = ListBuffer[Tuple2[Int,String]]()

    data2.append((1,"beijing"))
    data2.append((2,"shanghai"))
    data2.append((3,"guangzhou"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)


    text1.union(text2).print()
  }

}
