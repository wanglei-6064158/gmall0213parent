package com.atguigu.gmall0213.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.DauInfo
import com.atguigu.gmall0213.realtime.util.{MyEsUtil, MyKafkaUtil, OffSetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisCluster}

import scala.collection.mutable.ListBuffer

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkconf,Seconds(5))
    val groupId = "dau_group"
    val topic = "GMALL_START"
    val offSetForKafka: Map[TopicPartition, Long] = OffSetManagerUtil.getOffSet(topic,groupId)
    var recordInputDStream : InputDStream[ConsumerRecord[String, String]] = null;
    if(offSetForKafka != null && offSetForKafka.size > 0)
      {
        recordInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offSetForKafka,groupId)
      }else {
        recordInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    println("1111")
    //获得每个分区得偏移量
    var offSetRanges: Array[OffsetRange] = null;
    val inputGetOffSetDStream: DStream[ConsumerRecord[String, String]] = recordInputDStream.transform {
      rdd => {
        println("3333")
        offSetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    println("22222")


    //TODO redis集群模式
    val jsonObjectDStream: DStream[JSONObject] = inputGetOffSetDStream.map(record => {
      val jsonString: String = record.value()
      val jsonObject: JSONObject = JSON.parseObject(jsonString)
      jsonObject
    })

    val jsonObjFilteredDStream: DStream[JSONObject] = jsonObjectDStream.mapPartitions(jsonObjItr => {
      val list: List[JSONObject] = jsonObjItr.toList
      println("去重前" + list.size)
      val jsonObjList = new ListBuffer[JSONObject]
      val jedisCluster: JedisCluster = RedisUtil.getJedisClusterClient
      for (jsonObj <- list) {
        val midV: String = jsonObj.getJSONObject("common").getString("mid")
        val ts: lang.Long = jsonObj.getLong("ts")
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
        val flag: lang.Long = jedisCluster.sadd("dau:" + dt,midV)
        if (flag == 1L) {
          jsonObjList.append(jsonObj)
        }
      }
      println("去重后" + jsonObjList.size)
      jsonObjList.toIterator
    })

    val formattar = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    jsonObjFilteredDStream.foreachRDD{rdd =>
//       rdd.foreach(jsonObj=>println(jsonObj))
      rdd.foreachPartition{
        jsonObjItr => {
          val jsonObjList1: List[JSONObject] = jsonObjItr.toList
          for(j <- jsonObjList1)
            {
              println(j)
            }
          val dauWithIdList: List[(DauInfo, String)] = jsonObjList1.map(jsonObj => {
            val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
            val ts: lang.Long = jsonObj.getLong("ts")
            val dateTimeStr: String = formattar.format(ts)
            val dateTimeArr: Array[String] = dateTimeStr.split(" ")
            val dt: String = dateTimeArr(0)
            val time: String = dateTimeArr(1)
            val timeArr: Array[String] = time.split(":")
            val hr: String = timeArr(0)
            val mi: String = timeArr(1)
            val dauInfo = DauInfo(
              commonJsonObj.getString("mid"),
              commonJsonObj.getString("uid"),
              commonJsonObj.getString("ar"),
              commonJsonObj.getString("ch"),
              commonJsonObj.getString("vc"),
              dt, hr, mi, ts
            )
            (dauInfo, dauInfo.mid)
          })
          var dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
          if(null != dauWithIdList && dauWithIdList.size > 0)
            {
              println("有数据")
              dt = dauWithIdList(0)._1.dt
            }

          println(dt)
          MyEsUtil.bulkSave(dauWithIdList,"gmall_dau_info_" + dt)
        }
      }
      OffSetManagerUtil.saveOffSet(topic,groupId,offSetRanges)
    }

    //TODO 单机模式
//    val filterStream: DStream[String] = recordInputDStream.transform {
//          rdd => {
//            rdd.mapPartitions{
//                      datas =>{
//                        val strings: Iterator[String] = datas.map(_.value())
//                        val dataPartitionList: List[String] = strings.toList
//                        println("去重前" + dataPartitionList.size)
//                        val jedis: Jedis = RedisUtil.getJedisClient
//                        val redisKey = "dau"
//                        val filteredList = new ListBuffer[String]
//                        for(string <- dataPartitionList)
//                          {
//                            val jsonObject: JSONObject = JSON.parseObject(string)
//                            val midV: String = jsonObject.getJSONObject("common").getString("mid")
//                            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(jsonObject.getLong("ts")))
//                            val i: lang.Long = jedis.sadd(redisKey+dt, midV)
//                            if (i == 1L) {
//                              filteredList.append(midV)
//                            }
//                          }
//                        jedis.close()
//                        println("去重后" + filteredList.size)
//                        filteredList.toIterator
//                      }
//            }
//          }
//    }
//    filterStream.print(1000)
    ssc.start()
    ssc.awaitTermination()
  }

}
