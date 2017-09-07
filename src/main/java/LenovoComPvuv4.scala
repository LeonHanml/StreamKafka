
//import com.redis.RedisClient

import java.text.SimpleDateFormat

import Util.{KafkaConf, KafkaManager, MySqlPool, RedisClient,tools}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object LenovoComPvuv4 {

  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    val spark = SparkSession.builder().appName("1000001_Lenovo.comPvUvRcUn").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    val km = new KafkaManager(KafkaConf.kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, KafkaConf.kafkaParams, KafkaConf.topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //处理消息
        processRdd( rdd)
        //更新offsets
        km.updateZKOffsets(rdd)
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def processRdd(rdd: RDD[(String, String)]): Unit = {

    val data = rdd.map(_._2)

    val result = data.filter(line => Filter(line)
    ).map { line =>
      var vars = line.split("\\|_\\|")
      val datetime = vars(2).split("T")
      val date = datetime(0).mkString
      val time = datetime(1).split("\\+")(0).mkString.substring(0, 5) + ":00"

      val urlString = vars(3)
      val dataMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      for (data <- urlString.split("\\?")(1).split("&")) {
        val dataList = data.split("=")
        if (dataList.size == 2) {
          dataMap(dataList(0)) = dataList(1)
        } else if (dataList.size == 1) {
          dataMap(dataList(0)) = ""
        }
      }
      var ws = if (dataMap.contains("WS")) dataMap("WS") else "00000000"
      var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "11111111"
      var rc = if (dataMap.contains("RC")) dataMap("RC") else "1"
      var un = if (dataMap.contains("UN")) dataMap("UN") else ""
      (date, time, ws, cuc, rc, un)
    }

    result.foreachPartition { partition =>
      val jedis = RedisClient.pool.getResource

      jedis.select(11)
      val conn = MySqlPool.getJdbcConn()
      val stmt = conn.createStatement()
      val pvuvTable = "shopflow_total_pvuv"
      val pvuvIncreTable = "shopflow_total_incre_pvuv"

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")

      partition.foreach { line =>
        try {
          val date = line._1.toString()
          val time = line._2.toString()
          //Filter 函数已经验证  ws =10000001 即URL 为 "lenovo.com"
          val url = "lenovo.com"
          val cuc = line._4.toString()
          val rcStr = line._5.toString()
          val unStr = line._6.toString()

          val pvKey = s"PV:$date:$url"
          val uvKey = s"UV:$date:$url"
          val rcKey = s"RC:$date:$url"
          val unKey = s"UN:$date:$url"

          jedis.hincrBy(s"PV:$date:$url", url, 1)
          jedis.sadd(s"UV:$date:$url", url + cuc)

          if (rcStr.equals("0")) {
            jedis.sadd(s"RC:$date:$url", url + cuc)
          }
          jedis.sadd(s"UN:$date:$url", url + unStr)

          val minTSlogTimeMillions = tools.dateToStamp(s"$date $time").toLong

          val insertKey = "State_" + date

          val stateKey = "shopflowUvByHourState"
          if (jedis.exists(stateKey)) {
            val state = jedis.get(stateKey).toLong

            if (minTSlogTimeMillions > state) {

              val pvNum = jedis.hget(pvKey, url).toInt
              val uvNum = jedis.scard(uvKey).toInt
              val rcNum = jedis.scard(rcKey).toInt
              val unNum = jedis.scard(unKey).toInt

              var pvtemp = 0
              var uvtemp = 0
              var rctemp = 0
              var untemp = 0

              val timeOneMinBefore = sdf.parse(s"$date $time")
              timeOneMinBefore.setMinutes(timeOneMinBefore.getMinutes - 1)
              val timeOneMinBeforeString = sdf.format(timeOneMinBefore)

              val sqlOneMinuteBefore = s"select uv,pv,rc,un from $pvuvTable  where insert_time ='$timeOneMinBeforeString'"

              val rs = stmt.executeQuery(sqlOneMinuteBefore)
              if (rs.next()) {
                uvtemp = rs.getInt("uv")
                pvtemp = rs.getInt("pv")
                rctemp = rs.getInt("rc")
                untemp = rs.getInt("un")
              }
              val pvIncre = pvNum - pvtemp
              val uvIncre = uvNum - uvtemp
              val rcIncre = rcNum - rctemp
              val unIncre = unNum - untemp

              val sql = s"update $pvuvTable set pv=$pvNum, uv=$uvNum, rc=$rcNum, un=$unNum where insert_time = '$date $time'"

              val sqlIncre = s"update $pvuvIncreTable set pv=$pvIncre, uv=$uvIncre, rc=$rcIncre, un=$unIncre where insert_time = '$date $time'"

              stmt.execute(sql)
              stmt.execute(sqlIncre)

              jedis.set(stateKey, minTSlogTimeMillions.toString())
              jedis.lpush(insertKey, "1")
            }

          } else {
            jedis.set(stateKey, 0.toString)
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


  def Filter(line: String): Boolean = {

    var FilterFlag = false
    try {
      if (line.contains("WS=10000001&") && (!line.contains("CLE")) && (!line.toLowerCase().contains("spider")) && ifShopFlow(line)) {
        FilterFlag = true
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    return FilterFlag

  }

  def ifShopFlow(line: String): Boolean = {
    var shopFlow = false
    try {
      val dataList: Array[String] = line.split("\\|_\\|")
      val timeString = dataList(2).split("\\+")(0).replace("T", " ")
      val currentDay = timeString.split("T")(0).replace("-", "")
      val urlString = dataList(3)
      var dataMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      dataMap = tools.dataSplit(urlString)

      val pagesite = dataMap("PS")
      val pageurl = java.net.URLDecoder.decode(java.net.URLDecoder.decode(java.net.URLDecoder.decode(dataMap("PU"))))

      //println(s"pagesite: $pagesite, pageurl: $pageurl")

      val lenovoMallSite = List("www.lenovo.com.cn", "cart.lenovo.com.cn", "order.lenovo.com.cn", "i.lenovo.com.cn", "z.lenovo.com.cn", "s.lenovo.com.cn", "coupon.lenovo.com.cn", "m.lenovo.com.cn", "m.cart.lenovo.com.cn", "3g.lenovo.com.cn", "app_host_name", "m.order.lenovo.com.cn", "m.coupon.lenovo.com.cn", "buy.lenovo.com.cn", "mbuy.lenovo.com.cn", "cashier.lenovo.com.cn")
      val thinkMallSite = List("www.thinkworldshop.com.cn", "cart.thinkworldshop.com.cn", "thinkpad.lenovo.com.cn", "order.thinkworldshop.com.cn", "i.thinkworldshop.com.cn", "mobile.thinkworldshop.com.cn", "m.cart.thinkworldshop.com.cn", "s.thinkworldshop.com.cn", "coupon.thinkworld.com.cn", "m.coupon.thinkworldshop.com.cn", "m.order.thinkworldshop.com.cn")

      //联想商城
      if (lenovoMallSite.contains(pagesite)
        || (pagesite.equals("pay.i.lenovo.com") && (pageurl.contains("plat=4") || pageurl.contains("b2cPc_url") || pageurl.contains("plat=1") || pageurl.contains("b2cWap_url") || pageurl.contains("plat=3")))
        || (pagesite.equals("c.lenovo.com.cn") && pageurl.contains("lenovo"))
        || (pageurl.toLowerCase().equals("shopid=1"))) {
        shopFlow = true

      }
      //Think商城
      if (thinkMallSite.contains(pagesite)
        || (pagesite.equals("pay.i.lenovo.com") && (pageurl.contains("plat=5") || pageurl.contains("tkPc_url") || pageurl.contains("plat=8") || pageurl.contains("tkWap_url")))
        || (pagesite.equals("c.lenovo.com.cn") && pageurl.contains("think"))
        || (pageurl.toLowerCase().equals("shopid=2"))) {
        shopFlow = true
      }

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    shopFlow
  }


}
