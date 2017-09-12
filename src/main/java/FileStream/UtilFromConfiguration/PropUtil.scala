package FileStream.UtilFromConfiguration

/**
  * Created by Administrator on 2017/7/27 0027.
  */

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.util.Properties

object PropUtil {


//    val path = Thread.currentThread().getContextClassLoader.getResource("pvuv.properties").getPath //文件要放到resource文件夹下
//    val path ="/home/hanliang3/node/pvuv.properties"


  def getProperty(p:String): String ={
    val properties = new Properties()
    properties.load(new FileInputStream("pvuv.properties"))


//    val in: InputStream = getClass.getResourceAsStream("/pvuv.properties")
//    properties.load(new BufferedInputStream(in))


    return properties.getProperty(p)
  }
  def main(args: Array[String]): Unit = {
    println(PropUtil.getProperty("appName"))
    println(PropUtil.getProperty("redis.host"))
    println(PropUtil.getProperty("redis.auth"))
    RedisClient.keys()
  }

}
