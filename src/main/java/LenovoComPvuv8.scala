
//import com.redis.RedisClient

import java.text.SimpleDateFormat

import Util.{KafkaConf, KafkaManager, MySqlPool, RedisClient, tools}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/***
  * 计算common 以及 activity 流式处理
  * 代码内聚
  */
object LenovoComPvuv8 {

  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    val spark = SparkSession.builder().appName("1000001_Lenovo.com").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

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
//    val ttMap = Map(("tt1", "联想商城周年庆，购爆款， GO狂欢，更有秒杀低至1折起，赶快来分享！"), ("tt2", "联想平板电脑TAB4/TAB4 Plus新品发布"))

    val ttMap = Map(("联想商城周年庆，购爆款， GO狂欢，更有秒杀低至1折起，赶快来分享！", "tt1"), ("联想平板电脑TAB4/TAB4 Plus新品发布", "tt2"))
    val result = data.filter(line => (!line.toLowerCase().contains("spider"))).map(line => tools.dataSplitFromLine(line))
      .filter(dataMap => tools.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (!dataMap.contains("CLE")))


    result.foreachPartition { partition =>

      val jedis = RedisClient.pool.getResource
      jedis.select(11)
      val stateKey = "shopflowUvByHourState"
      val previous = "previous"
      val conn = MySqlPool.getJdbcConn()

      val pvuvTable = "shopflow_total_pvuv"
      val pvuvIncreTable = "shopflow_total_incre_pvuv"

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

          val puStr = if (dataMap.contains("PU")) tools.urlDecoder(dataMap("PU")) else "PU-NULL"
          var ttStr = if (dataMap.contains("TT")) tools.urlDecoder(dataMap("TT")) else "TT-NULL"
//          ("联想平板电脑TAB4/TAB4 Plus新品发布", "tt2")
          val ttCode  = ttMap(ttStr)
          val pvKey = s"PV:$date:$url"
          val uvKey = s"UV:$date:$url"
          val rcKey = s"RC:$date:$url"
          val unKey = s"UN:$date:$url"

          val ttKey = s"TT:$date:$ttCode"

          jedis.hincrBy(pvKey, url, 1)
          jedis.sadd(uvKey, url + cuc)
          if (rcStr.equals("0")) {
            jedis.sadd(rcKey, url + cuc)
          }
          if (!unStr.equals("")) {
            jedis.sadd(unKey, url + unStr)
          }
          val pvKeytt = s"PV:$date:$ttCode"
          val uvKeytt = s"UV:$date:$ttCode"
          val rcKeytt = s"RC:$date:$ttCode"
          val unKeytt = s"UN:$date:$ttCode"
          if (puStr.contains("/activity/")) {

            jedis.hincrBy(pvKeytt, url, 1)
            jedis.sadd(uvKeytt, url + cuc)
            if (rcStr.equals("0")) {
              jedis.sadd(rcKeytt, url + cuc)
            }
            if (!unStr.equals("")) {
              jedis.sadd(unKeytt, url + unStr)
            }
          }


          val minTSlogTimeMillions = tools.dateToStamp(s"$date $time").toLong
          val insertKey = "State_" + date

          if (jedis.exists(stateKey)) {
            val state = jedis.get(stateKey).toLong

            if (minTSlogTimeMillions > state) {
              {
                val pvNum = jedis.hget(pvKey, url).toInt
                val uvNum = jedis.scard(uvKey).toInt
                val rcNum = jedis.scard(rcKey).toInt
                val unNum = jedis.scard(unKey).toInt
                val sql = s"update $pvuvTable set pv=$pvNum, uv=$uvNum, rc=$rcNum, un=$unNum where min = '$date $time'"
                stmt.execute(sql)
              }
              for (ttCode <- ttMap.values) {
                val tableName = s"shopflow_$ttCode"+"_pvuv"
                val pvKeytt = s"PV:$date:$ttCode"
                val uvKeytt = s"UV:$date:$ttCode"
                val rcKeytt = s"RC:$date:$ttCode"
                val unKeytt = s"UN:$date:$ttCode"

                val pvNumtt = jedis.hget(pvKeytt, url).toInt
                val uvNumtt = jedis.scard(uvKeytt).toInt
                val rcNumtt = jedis.scard(rcKeytt).toInt
                val unNumtt = jedis.scard(unKeytt).toInt

                val sql = s"update $tableName set pv=$pvNumtt, uv=$uvNumtt, rc=$rcNumtt, un=$unNumtt where min = '$date $time'"
                stmt.execute(sql)

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
