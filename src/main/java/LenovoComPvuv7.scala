
//import com.redis.RedisClient

import java.text.SimpleDateFormat

import Util.{KafkaConf, KafkaManager, MySqlPool, RedisClient, tools}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object LenovoComPvuv7 {

  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    val spark = SparkSession.builder().appName("1000008_10000071_pvuv").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    val km = new KafkaManager(KafkaConf.kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, KafkaConf.kafkaParams, KafkaConf.topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //处理消息
        processRdd(spark, rdd)
        //更新offsets
        km.updateZKOffsets(rdd)
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def processRdd(spark: SparkSession, rdd: RDD[(String, String)]): Unit = {

    val data = rdd.map(_._2)
    val wsArray = Array("10000071", "10000008")
    println("开始过滤数据")
    val result = data.filter(line => (!line.toLowerCase().contains("spider"))).map(line => tools.dataSplitFromLine(line))
      .filter(dataMap => wsArray.contains(if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL") && (!dataMap.contains("CLE")))

    //    val swsArray = Array("pc_think", "wap_think", "pc_central", "pc_motoclub")

    result.foreachPartition { partition =>

      val jedis = RedisClient.pool.getResource
//      print("开始连接Redis数据库")
      val status = jedis.select(12)
//      print(s"Redis数据库status: $status")
      val stateKey = "shopflowUvByHourState"
      val previous = "previous"
      val conn = MySqlPool.getJdbcConn()

      val pvuvTable = "shopflow_total_pvuv"
      val pvuvIncreTable = "shopflow_total_incre_pvuv"
      val wsTable = "shopflow_"
      val pvuvstr = "_pvuv"
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")

      partition.foreach { dataMap =>
        try {
          val stmt = conn.createStatement()
          val date = dataMap("date")
          val time = dataMap("time")
          var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
          val url = ws
          var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
          var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
          var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"

          val pvKey = s"PV:$date:$url"
          val uvKey = s"UV:$date:$url"
          val rcKey = s"RC:$date:$url"
          val unKey = s"UN:$date:$url"

          jedis.hincrBy(pvKey, url, 1)
          jedis.sadd(uvKey, url + cuc)
          if (rcStr.equals("0")) {
            jedis.sadd(rcKey, url + cuc)
          }
          if (!unStr.equals("")) {
            jedis.sadd(unKey, url + unStr)
          }


          val minTSlogTimeMillions = tools.dateToStamp(s"$date $time").toLong
          val insertKey = "State_" + date

          if (jedis.exists(stateKey)) {
            val state = jedis.get(stateKey).toLong

            if (minTSlogTimeMillions > state) {

              for (url <- wsArray) {
                val pvKey = s"PV:$date:$url"
                val uvKey = s"UV:$date:$url"
                val rcKey = s"RC:$date:$url"
                val unKey = s"UN:$date:$url"
                if (jedis.exists(pvKey)) {
                  val pvNum = if ( jedis.hget(pvKey, url) != null) jedis.hget(pvKey, url).toInt else 0
                  val uvNum = if (jedis.exists(uvKey) && jedis.scard(uvKey) != null) jedis.scard(uvKey).toInt else 0
                  val rcNum = if (jedis.exists(rcKey) && jedis.scard(rcKey) != null) jedis.scard(rcKey).toInt else 0
                  val unNum = if (jedis.exists(unKey) && jedis.scard(unKey) != null) jedis.scard(unKey).toInt else 0

                  //date='$date'      shopflow_ _pvuv
                  val sql = s"update $wsTable$url$pvuvstr set pv=$pvNum, uv=$uvNum, rc=$rcNum, un=$unNum where min = '$date $time'"

                  println(s"sql: $sql")
                  stmt.execute(sql) //add to redis
                }

              }
              jedis.set(stateKey, minTSlogTimeMillions.toString())
              jedis.lpush(insertKey, "1")
            }

          } else {


            jedis.set(stateKey, minTSlogTimeMillions.toString())
            jedis.lpush(insertKey, "1")
          }
        }
        catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
      MySqlPool.releaseConn(conn)
      jedis.close()
    }
  }


}
