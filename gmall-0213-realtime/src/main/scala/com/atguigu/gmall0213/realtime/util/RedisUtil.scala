package com.atguigu.gmall0213.realtime.util

import org.apache.spark.internal.config
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool, JedisPoolConfig, JedisSentinelPool}

import scala.collection.mutable

object RedisUtil {

  var jedisPool:JedisPool=null

  def getJedisClient: Jedis = {
    if(jedisPool==null){
      //      println("开辟一个连接池")
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)   //最大空闲
      jedisPoolConfig.setMinIdle(20)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    //    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    //   println("获得一个连接")
    jedisPool.getResource
  }

  var jedisCluster :JedisCluster = null;

  def getJedisClusterClient : JedisCluster = {
    if(jedisCluster == null)
      {
        val set: java.util.Set[HostAndPort] = new java.util.HashSet()
        val config = PropertiesUtil.load("config.properties")
        val str: String = config.getProperty("redis.clusterHostsAndPorts")
        val hostAndPorts: Array[String] = str.split(",")
        for(hostAndPort <- hostAndPorts)
          {
            val hostAndPortSplit: Array[String] = hostAndPort.split(":")
            set.add(new HostAndPort(hostAndPortSplit(0),hostAndPortSplit(1).toInt))
          }
        val jedisPoolConfig = new JedisPoolConfig()
        jedisPoolConfig.setMaxTotal(100)  //最大连接数
        jedisPoolConfig.setMaxIdle(20)   //最大空闲
        jedisPoolConfig.setMinIdle(20)     //最小空闲
        jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
        jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
        jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
        jedisCluster = new JedisCluster(set,jedisPoolConfig)
      }
    jedisCluster
  }
}


//private  static JedisCluster jedisCluster = null;
//public static JedisCluster getJedisCluster()
//{
//  if(jedisCluster == null)
//{
//  Set<HostAndPort> set = new HashSet<>();
//  set.add(new HostAndPort("hadoop102",6379));
//  set.add(new HostAndPort("hadoop102",6380));
//  set.add(new HostAndPort("hadoop102",6381));
//  JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//  jedisPoolConfig.setMaxTotal(10); //最大可用连接数
//  jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
//  jedisPoolConfig.setMinIdle(5); //最小闲置连接数
//  jedisPoolConfig.setBlockWhenExhausted(true);//连接耗尽  是否等待
//  jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
//  jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping -> pong
//  JedisCluster jedisCluster = new JedisCluster(set,jedisPoolConfig);
//  return jedisCluster;
//}else{
//  return jedisCluster;
//}
//}
