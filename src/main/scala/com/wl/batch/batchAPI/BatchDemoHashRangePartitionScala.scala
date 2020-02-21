package com.wl.batch.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoHashRangePartitionScala {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data1 = ListBuffer[Tuple2[Int, String]]()

    data1.append((1, "ww1"))
    data1.append((2, "ww2"))
    data1.append((2, "ww3"))
    data1.append((2, "ww4"))
    data1.append((3, "ww5"))
    data1.append((3, "ww6"))
    data1.append((3, "ww7"))
    data1.append((4, "ww8"))
    data1.append((4, "ww9"))
    data1.append((4, "ww10"))
    data1.append((5, "ww11"))
    data1.append((5, "ww12"))
    data1.append((6, "ww13"))
    data1.append((6, "ww14"))
    data1.append((6, "ww15"))

    val text1= env.fromCollection(data1)

    text1.partitionByHash(0).mapPartition(it=>{
      while(it.hasNext){
        val tu = it.next()
        println("当前线程ID:"+Thread.currentThread().getId+","+tu)
      }
      it
    }).print()

  }

}
