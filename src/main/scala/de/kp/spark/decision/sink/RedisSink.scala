package de.kp.spark.decision.sink
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

import java.util.Date

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisClient

import de.kp.spark.decision.Configuration
import scala.collection.JavaConversions._

class RedisSink {

  val (host,port) = Configuration.redis
  val client = RedisClient(host,port.toInt)

  val service = "decision"

  def addForest(req:ServiceRequest, forest:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "forest:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + forest
    
    client.zadd(k,timestamp,v)
    
  }
   
  def forestExists(uid:String):Boolean = {

    val k = "forest:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def forest(uid:String):String = {

    val k = "forest:" + service + ":" + uid
    val forests = client.zrange(k, 0, -1)

    if (forests.size() == 0) {
      null
    
    } else {
      
      val last = forests.toList.last
      last.split(":")(1)
      
    }
  
  }

}