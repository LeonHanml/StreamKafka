
//import com.redis.RedisClient

import java.text.SimpleDateFormat

import Util.{KafkaConf, KafkaManager, MySqlPool, RedisClient, tools}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//子站点pvuv计算
object LenovoComPvuv6 {

  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    val spark = SparkSession.builder().appName("1000001_Lenovo.com").enableHiveSupport().getOrCreate()
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


  def setKeyIntoRedis(jedis: Jedis, url: String): Unit = {

    //    jedis.hincrBy(pvKey, url, 1)
    //    jedis.sadd(uvKey, url + cuc)
    //    if (rcStr.equals("0")) {
    //      jedis.sadd(rcKey, url + cuc)
    //    }
    //    if (!unStr.equals("")) {
    //      jedis.sadd(unKey, url + unStr)
  }


  def processRdd(spark: SparkSession, rdd: RDD[(String, String)]): Unit = {

    val data = rdd.map(_._2)

    val result = data.filter(line => (!line.toLowerCase().contains("spider"))).map(line => tools.dataSplitFromLine(line))
      .filter(dataMap => tools.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (!dataMap.contains("CLE")))

    //    dataMap("SWSPID")
    result.foreachPartition { partition =>

      val jedis = RedisClient.pool.getResource
      jedis.select(11)
      val stateKey = "shopflowUvByHourState"
      val previous = "previous"

      val conn = MySqlPool.getJdbcConn()
      val stmt = conn.createStatement()

      val pvuvTable = "shopflow_total_pvuv"
      val pvuvIncreTable = "shopflow_total_incre_pvuv"

      val swspidTable = "shopflow_sws_pvuv"

      val swsArray = Array("pc_think", "wap_think", "pc_central","pc_motoclub")


      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")

      partition.foreach { dataMap =>
        try {
          val date = dataMap("date")
          val time = dataMap("time")
          var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
          val url = "lenovo.com"
          var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
          var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "1"
          var unStr = if (dataMap.contains("UN")) dataMap("UN") else "1"

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

          val swsUrl = if (dataMap.contains("SWSPID")) dataMap("SWSPID") else "SWSPID-NULL"
          val swspvKey = s"PV:$date:$swsUrl"
          val swsuvKey = s"UV:$date:$swsUrl"
          val swsrcKey = s"RC:$date:$swsUrl"
          val swsunKey = s"UN:$date:$swsUrl"
          if (swsArray.contains(swsUrl)) {
            jedis.hincrBy(swspvKey, swsUrl, 1)
            jedis.sadd(swsuvKey, swsUrl + cuc)
            if (rcStr.equals("0")) {
              jedis.sadd(swsrcKey, swsUrl + cuc)
            }
            if (!unStr.equals("")) {
              jedis.sadd(swsunKey, swsUrl + unStr)
            }
          }

          val minTSlogTimeMillions = tools.dateToStamp(s"$date $time").toLong
          val insertKey = "State_" + date

          if (jedis.exists(stateKey)) {
            val state = jedis.get(stateKey).toLong

            if (minTSlogTimeMillions > state) {

              //              主站数据
              val pvNum = jedis.hget(pvKey, url).toInt
              val uvNum = jedis.scard(uvKey).toInt
              val rcNum = jedis.scard(rcKey).toInt
              val unNum = jedis.scard(unKey).toInt


              //date='$date'
              val sql = s"update $pvuvTable set pv=$pvNum, uv=$uvNum, rc=$rcNum, un=$unNum where min = '$date $time'"
              //              val sqlIncre = s"update $pvuvIncreTable set pv=$pvIncre, uv=$uvIncre, rc=$rcIncre, un=$unIncre where min = '$date $time'"



              stmt.execute(sql)

              //              stmt.execute(sqlIncre)
              //              子站数据
              for ( swsUrl <- swsArray){
//                val swsUrl = if (dataMap.contains("SWSPID")) dataMap("SWSPID") else "SWSPID-NULL"
                val swspvKey = s"PV:$date:$swsUrl"
                val swsuvKey = s"UV:$date:$swsUrl"
                val swsrcKey = s"RC:$date:$swsUrl"
                val swsunKey = s"UN:$date:$swsUrl"

                val swspvNum = jedis.hget(swspvKey, swsUrl).toInt
                val swsuvNum = jedis.scard(swsuvKey).toInt
                val swsrcNum = jedis.scard(swsrcKey).toInt
                val swsunNum = jedis.scard(swsunKey).toInt

              }



              jedis.set(stateKey, minTSlogTimeMillions.toString())
              jedis.lpush(insertKey, "1")


            } else {
              val pvNum = jedis.hget(pvKey, url).toInt
              val uvNum = jedis.scard(uvKey).toInt
              val rcNum = jedis.scard(rcKey).toInt
              val unNum = jedis.scard(unKey).toInt
              val sql = s"update $pvuvTable set pv=$pvNum, uv=$uvNum, rc=$rcNum, un=$unNum ,date=$date where min = '$date $time'"

              jedis.set(stateKey, minTSlogTimeMillions.toString())
              jedis.lpush(insertKey, "1")
            }
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
