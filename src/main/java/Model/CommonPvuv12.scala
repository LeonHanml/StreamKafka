package Model

/**
  * Created by Administrator on 2017/8/23 0023.
  */

import java.sql.{Connection, Statement}
import java.util.regex.Pattern

import Util.tools
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

/**
  * 过滤条件修改为URL包含 activity
  * @param dataMap
  * @param jedis
  * @param conn
  */
class CommonPvuv12(dataMap: Map[String, String], jedis: Jedis, conn: Connection) extends Serializable{

  //  def this(dataMap: Map[String, String]) {
  //    this(dataMap: Map[String, String],null)
  //  }
  //  val dataMap = dataMap
  val date:String = dataMap("date")
  val time:String = dataMap("time")
  var ws:String = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
  val url:String = ws

  var cuc:String = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
  var rcStr:String = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
  var unStr:String = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"
  //  val conn = MySqlPool.getJdbcConn()

  var cleStr :String =if (dataMap.contains("CLE")) dataMap("CLE") else "CLE-NULL"

  @transient val stmt:Statement = conn.createStatement()


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
//      if (puStr.contains("activity")) {
        var ttMap = Map(("联想商城周年庆，购爆款， GO狂欢，更有秒杀低至1折起，赶快来分享！", "tt1"), ("联想平板电脑TAB4/TAB4 Plus新品发布", "tt2"))

//        给每一个活动名称一个编号
        var ttcodeNum = 3
        if (jedis.exists("ttcodeNum")){
          ttcodeNum = jedis.get("ttcodeNum").toInt
        }
        if(jedis.exists("ttMap")){
          ttMap = jedis.hgetAll("ttMap").asScala
        }
        if (ttMap.contains(ttStr)) {
          val ttCode = ttMap(ttStr)
          val pvKeytt = s"PV:$date:$ttCode"
          val uvKeytt = s"UV:$date:$ttCode"
          val rcKeytt = s"RC:$date:$ttCode"
          val unKeytt = s"UN:$date:$ttCode"


          jedis.hincrBy(pvKeytt, url, 1)
          jedis.sadd(uvKeytt, url + cuc)
          if (rcStr.equals("0")) {
            jedis.sadd(rcKeytt, url + cuc)
          }
          if (!unStr.equals("")) {
            jedis.sadd(unKeytt, url + unStr)
          }

        } else {
          ttcodeNum = ttcodeNum + 1
          jedis.set("ttcodeNum",ttcodeNum.toString)
          jedis.hset("ttMap",ttStr,s"tt$ttcodeNum")
//          jedis.sadd(s"$date:ttset", ttStr)
        }
//      }
    }catch {
      case ex: IllegalArgumentException =>
        var puStr = "PU-NULL"
        var ttStr ="TT-NULL"

      case ex: Exception =>
        ex.printStackTrace()

    }

  }

  def getActivityPvuv(): Unit = {
    try {
      var ttMap = Map(("联想商城周年庆，购爆款， GO狂欢，更有秒杀低至1折起，赶快来分享！", "tt1"), ("联想平板电脑TAB4/TAB4 Plus新品发布", "tt2"))

      if (jedis.exists("ttMap")) {
        ttMap = jedis.hgetAll("ttMap").asScala
      }
      val tableName = s"shopflow_tt_pvuv"
      for (ttName <- ttMap.keys) {

        val ttCode = ttMap(ttName)
        if (jedis.exists(s"PV:$date:$ttCode")) {

          //          val tableName = s"shopflow_$ttCode" + "_pvuv"
          val pvKeytt = s"PV:$date:$ttCode"
          val uvKeytt = s"UV:$date:$ttCode"
          val rcKeytt = s"RC:$date:$ttCode"
          val unKeytt = s"UN:$date:$ttCode"

          val pvNumtt = jedis.hget(pvKeytt, url).toInt
          val uvNumtt = jedis.scard(uvKeytt).toInt
          val rcNumtt = jedis.scard(rcKeytt).toInt
          val unNumtt = jedis.scard(unKeytt).toInt
          //s"insert $tableName set pv=$pvNumtt, uv=$uvNumtt, rc=$rcNumtt, un=$unNumtt where min = '$date $time'"
          val insertsql = s"INSERT INTO shopflow_tt_pvuv (date,ttcode,ttname,pv,uv,rc,un,min) values ('$date','$ttCode','$ttName',$pvNumtt,$uvNumtt,$rcNumtt,$unNumtt,'$date $time')"
          val updatesql = s"update $tableName set pv=$pvNumtt, uv=$uvNumtt, rc=$rcNumtt, un=$unNumtt,min='$date $time' where date = '$date' and ttcode = '$ttCode' "
//          println(insertsql)
//          println(updatesql)


          val count = stmt.executeUpdate(updatesql)
          if (count <= 0) {
            stmt.executeUpdate(insertsql)
          }


        }
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()

    }
  }

  def setGoodStore(): Unit = {
    val pattern = """.*\\|\\|show::goodsstore::[1-9]\d*::\d::wap\\|\\|.*""".r
    if (cleStr.equals("CLE-NULL")) return
    val strs = (pattern findAllIn cleStr).mkString
    if (strs.length() < 5) {
      return
    }
    val strArray = strs.split("::")
    val pcode = strArray(2)
    val state = strArray(3)
    val port = strArray(4)

//    val gsKey = s"goodstore:$date:$port:$pcode"
    jedis.hset(s"goodstore:$date",s"$port:$pcode",state)


  }

  def getGoodStore(): Unit ={
    jedis.hgetAll(s"goodstore:$date")
  }


}



