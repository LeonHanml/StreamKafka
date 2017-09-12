package FileStream.UtilFromConfiguration

import java.sql.{Connection, Statement}

import redis.clients.jedis.Jedis

import scala.collection.mutable.Map

/**
  * Created by Administrator on 2017/9/12 0012.
  */
class CommonPvuv(dataMap: Map[String, String], jedis: Jedis, conn: Connection) extends Serializable{
  val date:String = dataMap("date")
  val time:String = dataMap("time")
  var ws:String = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
  val url:String = ws

  var cuc:String = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
  var rcStr:String = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
  var unStr:String = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"
  //  val conn = MySqlPool.getJdbcConn()

  @transient val stmt:Statement = conn.createStatement()

  def getPv(): Unit ={


  }

}
