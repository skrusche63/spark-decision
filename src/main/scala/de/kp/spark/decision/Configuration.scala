package de.kp.spark.decision
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Spark-Decision project
 * (https://github.com/skrusche63/spark-decision).
 * 
 * Spark-Decision is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * Spark-Decision is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * Spark-Decision. 
 * 
 * If not, see <http://www.gnu.org/licenses/>.
 */

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.{Configuration => HConf}

import de.kp.spark.core.{Configuration => CoreConf}

object Configuration extends CoreConf {

    /* Load configuration for router */
  val path = "application.conf"
  val config = ConfigFactory.load(path)

  override def actor:(Int,Int,Int) = {
  
    val cfg = config.getConfig("actor")

    val duration = cfg.getInt("duration")
    val retries = cfg.getInt("retries")  
    val timeout = cfg.getInt("timeout")
    
    (duration,retries,timeout)
    
  }

  override def elastic:HConf = {
  
    val cfg = config.getConfig("elastic")
    val conf = new HConf()                          

    conf.set("es.nodes",cfg.getString("es.nodes"))
    conf.set("es.port",cfg.getString("es.port"))

    conf.set("es.resource", cfg.getString("es.resource"))                
    conf.set("es.query", cfg.getString("es.query"))                          
 
    conf
    
  }
   
  override def file:List[String] = {
  
    val cfg = config.getConfig("file")
    List(cfg.getString("path"))  
    
  }
 
  override def mysql:(String,String,String,String) = {

   val cfg = config.getConfig("mysql")
  
   val url = cfg.getString("url")
   val db  = cfg.getString("database")
  
   val user = cfg.getString("user")
   val password = cfg.getString("password")
    
   (url,db,user,password)
   
  }
  
  override def redis:(String,String) = {
  
    val cfg = config.getConfig("redis")
    
    val host = cfg.getString("host")
    val port = cfg.getString("port")
    
    (host,port)
    
  }

  override def rest:(String,Int) = {
      
    val cfg = config.getConfig("rest")
      
    val host = cfg.getString("host")
    val port = cfg.getInt("port")

    (host,port)
    
  }
  
  override def spark:Map[String,String] = {
  
    val cfg = config.getConfig("spark")
    
    Map(
      "spark.executor.memory"          -> cfg.getString("spark.executor.memory"),
	  "spark.kryoserializer.buffer.mb" -> cfg.getString("spark.kryoserializer.buffer.mb")
    )

  }
  
  def tree:(String,String) = {
  
    val cfg = config.getConfig("tree")
    
    val info = cfg.getString("info")      
    val base = cfg.getString("base")   
    
    (base,info)
    
  }
  
}