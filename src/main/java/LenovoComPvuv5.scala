
//import com.redis.RedisClient

import java.text.SimpleDateFormat

import Util.{KafkaConf, KafkaManager, MySqlPool, RedisClient, tools}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object LenovoComPvuv5 {

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

//      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")

      partition.foreach { dataMap =>
        try {
          val stmt =  conn.createStatement()
          val date = dataMap("date")
          val time = dataMap("time")
          var ws = if (dataMap.contains("WS")) dataMap("WS") else "00000000"
          val url = "lenovo.com"
          var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "11111111"
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


          val minTSlogTimeMillions = tools.dateToStamp(s"$date $time").toLong
          val insertKey = "State_" + date

          if (jedis.exists(stateKey)) {
            val state = jedis.get(stateKey).toLong

            if (minTSlogTimeMillions > state) {
              var pvPrevious = 0
              var uvPrevious = 0
              var rcPrevious = 0
              var unPrevious = 0
              if (jedis.exists(previous)) {
                pvPrevious = jedis.hget(previous, "pvPrevious").toInt
                uvPrevious = jedis.hget(previous, "uvPrevious").toInt
                rcPrevious = jedis.hget(previous, "rcPrevious").toInt
                unPrevious = jedis.hget(previous, "unPrevious").toInt
              }
              val pvNum = jedis.hget(pvKey, url).toInt
              val uvNum = jedis.scard(uvKey).toInt
              val rcNum = jedis.scard(rcKey).toInt
              val unNum = jedis.scard(unKey).toInt

              val pvIncre = pvNum - pvPrevious
              val uvIncre = uvNum - uvPrevious
              val rcIncre = rcNum - rcPrevious
              val unIncre = unNum - unPrevious
              //date='$date'
              val sql = s"update $pvuvTable set pv=$pvNum, uv=$uvNum, rc=$rcNum, un=$unNum where min = '$date $time'"
              val sqlIncre = s"update $pvuvIncreTable set pv=$pvIncre, uv=$uvIncre, rc=$rcIncre, un=$unIncre where min = '$date $time'"
//              println(sql)
              jedis.hset(previous, "pvPrevious", pvNum.toString)
              jedis.hset(previous, "uvPrevious", uvNum.toString)
              jedis.hset(previous, "rcPrevious", rcNum.toString)
              jedis.hset(previous, "unPrevious", unNum.toString)

              stmt.execute(sql) //add to redis
              stmt.execute(sqlIncre)

              jedis.set(stateKey, minTSlogTimeMillions.toString())
              jedis.lpush(insertKey, "1")
            }

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
