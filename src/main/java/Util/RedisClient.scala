package Util



import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

import scala.collection.JavaConversions._

object RedisClient extends Serializable {

  def main(args: Array[String]): Unit = {
    keys()
  }

  val redisHost = "10.250.100.20"
  val redisPort = 6379
  val redisTimeout = 30000
  val auth = "lenovo-1234"

  var config = new  GenericObjectPoolConfig()
//  config.setMaxTotal(50)
//  val poolConfig=new JedisPoolConfig()
  config.setMaxTotal(200)
  config.setMaxIdle(50)
  config.setMinIdle(8) //设置最小空闲数

  config.setMaxWaitMillis(10000)
  config.setTestOnBorrow(true)
  config.setTestOnReturn(true)
  //Idle时进行连接扫描//Idle时进行连接扫描

  config.setTestWhileIdle(true)
  //表示idle object evitor两次扫描之间要sleep的毫秒数
  config.setTimeBetweenEvictionRunsMillis(30000)
  //表示idle object evitor每次扫描的最多的对象数
  config.setNumTestsPerEvictionRun(10)
  //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
  config.setMinEvictableIdleTimeMillis(60000)


  lazy val pool = new JedisPool(config, redisHost, redisPort, redisTimeout, auth)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

  def keys(): Unit = {
    val dbIndex = 13
    val jedis = RedisClient.pool.getResource
    jedis.select(dbIndex)
    val keys = jedis.keys("*")
    for (k <- keys) {
      println(k)
      //      jedis.hgetAll(k)

    }
    //    jedis.del()

  }
}



