
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
import com.mysql.jdbc

import scala.collection.JavaConversions._
import java.sql.{DriverManager, ResultSet, SQLException, Statement}

import Util.RedisClient
import com.mysql.jdbc.Connection

object RedisClientLocal extends Serializable {
  val redisHost = "10.250.100.20"
  val redisPort = 6379
  val redisTimeout = 30000
  val auth = "lenovo-1234"
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout, auth)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

  def runAll(): Unit = {
    val dbIndex = 10
    val jedis = RedisClient.pool.getResource
    jedis.select(dbIndex)
    val stmt = connectMysql()
    stmt.execute("")

    val day = ""

    val keys = jedis.keys("*")

    for (k <- keys) {
      val karr = k.split(":")
      val date = karr(1)
      val ws = karr(2)
      if (date == day) {
        if (k.contains("UV")) {
          // println(k +"--"+ jedis.scard(k.toString))
          val uv = jedis.scard(k.toString).toInt
          val sql = "insert into tpvuv ('date','ws','uv') values (" + date + "," + ws + "," + uv + ")"
          println(sql)
          stmt.execute(sql)
        }
        if (k.contains("PV")) {
          //println(jedis.hget("PV:20170707:10000138","10000138"))
          val pv = jedis.hget(k, ws).toInt
          // val sql = "insert into tpvuv ('date','ws','uv') values ("+date.toString+" , "+ws.toString+" , "+pv+")"
          val sql = "update tpvuv set pv = " + pv + " ,   where date = " + date + " and ws = " + ws
          println(sql)
          stmt.execute(sql)
        }
      }
    }


  }




  def connectMysql(): Statement = {
//    val url = "jdbc:mysql://10.116.39.130:3306/pvuv"
//    val url = "jdbc:mysql://10.116.30.66:3306/pvuv"
//    val prop = new java.util.Properties
//    prop.setProperty("user", "root")
//    prop.setProperty("password", "hanliang")
    try {
      //加载MySql的驱动类
      Class.forName("com.mysql.jdbc.Driver").newInstance()
    } catch {
      case e: NullPointerException => e.printStackTrace()

    }


    val conn = DriverManager.getConnection("jdbc:mysql://10.116.30.66:3306/pvuv", "root", "hanliang")
    val stmt = conn.createStatement()
    return stmt
  }

  def closeMysql(conn: Connection, stmt: Statement, rs: ResultSet): Unit = {
    if (rs != null) { // 关闭记录集
      try {
        rs.close();
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
    if (stmt != null) { // 关闭声明
      try {
        stmt.close();
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
    if (conn != null) { // 关闭连接对象
      try {
        conn.close();
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }
}



