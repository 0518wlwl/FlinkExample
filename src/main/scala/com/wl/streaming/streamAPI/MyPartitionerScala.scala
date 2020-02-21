package com.wl.streaming.streamAPI

import org.apache.flink.api.common.functions.Partitioner

class MyPartitionerScala extends Partitioner[Long]{

  override def partition(key: Long, numPartiitons: Int): Int = {
    println("分区总数:"+numPartiitons)

    if(key % 2 ==0){
      0
    }else{
      1
    }
  }
}
