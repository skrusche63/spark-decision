package de.kp.spark.decision.redis
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

import de.kp.spark.decision.model._

import java.util.Date
import scala.collection.JavaConversions._

object RedisCache {

  val client  = RedisClient()
  val service = "decision"

  def addForest(uid:String, forest:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "forest:" + service + ":" + uid
    val v = "" + timestamp + ":" + forest
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addMeta(uid:String,meta:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "meta:" + uid
    val v = "" + timestamp + ":" + meta
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addStatus(uid:String, task:String, status:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "job:" + service + ":" + uid
    val v = "" + timestamp + ":" + Serializer.serializeJob(JobDesc(service,task,status))
    
    client.zadd(k,timestamp,v)
    
  }
   
  def forestExist(uid:String):Boolean = {

    val k = "forest:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def metaExists(uid:String):Boolean = {

    val k = "meta:" + uid
    client.exists(k)
    
  }
  
  def taskExists(uid:String):Boolean = {

    val k = "job:" + service + ":" + uid
    client.exists(k)
    
  }
  
  /**
   * Get timestamp when job with 'uid' started
   */
  def starttime(uid:String):Long = {
    
    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      0
    
    } else {
      
      val first = jobs.iterator().next()
      first.split(":")(0).toLong
      
    }
     
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
  
  def meta(uid:String):String = {

    val k = "meta:" + uid
    val metas = client.zrange(k, 0, -1)

    if (metas.size() == 0) {
      null
    
    } else {
      
      metas.toList.last
      
    }

  }
  
  def status(uid:String):String = {

    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      null
    
    } else {
      
      val job = Serializer.deserializeJob(jobs.toList.last)
      job.status
      
    }

  }

}