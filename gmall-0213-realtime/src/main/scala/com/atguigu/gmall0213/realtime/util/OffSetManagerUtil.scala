package com.atguigu.gmall0213.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.JedisCluster

import scala.collection.JavaConverters._
import scala.collection.mutable

object OffSetManagerUtil {

  def getOffSet(topic:String,consumerGroupId:String):Map[TopicPartition,Long] = {
      val jedisCluster: JedisCluster = RedisUtil.getJedisClusterClient
      val offSetKey = topic + ":" + consumerGroupId
      val offSetMap: util.Map[String, String] = jedisCluster.hgetAll(offSetKey)
      if(offSetMap != null && offSetMap.size() > 0)
        {
          val offSetList: List[(String, String)] = offSetMap.asScala.toList
          val offSetMapForKafka: Map[TopicPartition, Long] = offSetList.map {
            case (partition, offset) => {
              println("加载偏移量：分区" + partition + "==>" + offset)
              (new TopicPartition(topic, partition.toInt), offset.toLong)
            }
          }.toMap
          offSetMapForKafka
        }else {
        null
      }

  }

  def saveOffSet(topic:String,groupId:String,offSetRanges:Array[OffsetRange]) = {
    val offSetKey = topic + ":" + groupId
    val offSetMap : util.Map[String,String] = new util.HashMap[String,String]() //用来存储多个分区得偏移量
    if(offSetRanges != null && offSetRanges.size > 0)
      {
        val jedisCluster: JedisCluster = RedisUtil.getJedisClusterClient
        for(offSetRange <- offSetRanges)
        {
          val partition: String = offSetRange.partition.toString
          val offSet: String = offSetRange.untilOffset.toString
          println("写入偏移量：分区" + partition + "==>" + offSet)
          offSetMap.put(partition,offSet)
        }
        jedisCluster.hmset(offSetKey,offSetMap)
      }else {
      println("offSetRanges为空")
    }
  }
}
