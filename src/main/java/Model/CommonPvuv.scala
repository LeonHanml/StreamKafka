package Model

/**
  * Created by Administrator on 2017/8/23 0023.
  */

import java.sql.Connection

import Util.{MySqlPool, tools}
import redis.clients.jedis.Jedis

import scala.collection.mutable.Map

class CommonPvuv(dataMap: Map[String, String], jedis: Jedis, conn: Connection) extends Serializable{

  //  def this(dataMap: Map[String, String]) {
  //    this(dataMap: Map[String, String],null)
  //  }
  //  val dataMap = dataMap
  val date = dataMap("date")
  val time = dataMap("time")
  var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
  val url = ws

  var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
  var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
  var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"
  //  val conn = MySqlPool.getJdbcConn()

  @transient val stmt = conn.createStatement()

  val ttMap = Map(("联想商城周年庆，购爆款， GO狂欢，更有秒杀低至1折起，赶快来分享！", "tt1"), ("联想平板电脑TAB4/TAB4 Plus新品发布", "tt2"))

  def setCommonPvuv(): Unit = {

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
  }

  def getCommonPvuv(): Unit = {
    val pvKey = s"PV:$date:$url"
    val uvKey = s"UV:$date:$url"
    val rcKey = s"RC:$date:$url"
    val unKey = s"UN:$date:$url"

    val pvNum = jedis.hget(pvKey, url).toInt
    val uvNum = jedis.scard(uvKey).toInt
    val rcNum = jedis.scard(rcKey).toInt
    val unNum = jedis.scard(unKey).toInt
    val pvuvTable = "shopflow_total_pvuv"
    val sql = s"update $pvuvTable set pv=$pvNum, uv=$uvNum, rc=$rcNum, un=$unNum where min = '$date $time'"
    stmt.execute(sql)

  }

  def setActivityPvuv(): Unit = {

    try {
      var puStr = if (dataMap.contains("PU")) tools.urlDecoder(dataMap("PU")) else "PU-NULL"
      var ttStr = if (dataMap.contains("TT")) tools.urlDecoder(dataMap("TT")) else "TT-NULL"
      if (puStr.contains("/activity/")) {
        if (ttMap.contains(ttStr)) {
          val ttCode = ttMap(ttStr)
          val pvKeytt = s"PV:$date:$ttCode"
          val uvKeytt = s"UV:$date:$ttCode"
          val rcKeytt = s"RC:$date:$ttCode"
          val unKeytt = s"UN:$date:$ttCode"
          //      if (puStr.contains("/activity/")) {

          jedis.hincrBy(pvKeytt, url, 1)
          jedis.sadd(uvKeytt, url + cuc)
          if (rcStr.equals("0")) {
            jedis.sadd(rcKeytt, url + cuc)
          }
          if (!unStr.equals("")) {
            jedis.sadd(unKeytt, url + unStr)
          }

        } else {
          jedis.sadd(s"$date:ttset", ttStr)
        }
      }
    }catch {
      case ex: IllegalArgumentException => {
        var puStr = "PU-NULL"
        var ttStr ="TT-NULL"
      }
      case ex: Exception => {
        ex.printStackTrace()
      }
    }

  }

  def getActivityPvuv(): Unit = {
    try {
      for (ttCode <- ttMap.values) {
        if (jedis.exists(s"PV:$date:$ttCode")) {

          val tableName = s"shopflow_$ttCode" + "_pvuv"
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
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }
}



