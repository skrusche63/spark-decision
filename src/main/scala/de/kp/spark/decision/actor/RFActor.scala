package de.kp.spark.decision.actor
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.Date
import akka.actor.{Actor,ActorLogging}

import de.kp.spark.decision.Configuration
import de.kp.spark.decision.source.DecisionSource

import de.kp.spark.decision.model._
import de.kp.spark.decision.tree.RF

import de.kp.spark.decision.redis.RedisCache
import de.kp.spark.decision.util.Features

class RFActor(@transient val sc:SparkContext) extends Actor with ActorLogging {
  
  private val (base,info) = Configuration.tree
  
  def receive = {

    case req:ServiceRequest => {

      val uid = req.data("uid")
      val task = req.task
      
      val params = properties(req)
      val missing = (params == null)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        RedisCache.addStatus(uid,task,DecisionStatus.STARTED)
 
        try {

          val source = new DecisionSource(sc)
          val dataset = source.get(req.data)

          if (dataset != null) buildForest(uid,task,dataset,params) else null
          
        } catch {
          case e:Exception => RedisCache.addStatus(uid,task,DecisionStatus.FAILURE)          
        }
 
      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def buildForest(uid:String,task:String,dataset:RDD[Instance],params:(Int,Int,String)) {

    RedisCache.addStatus(uid,task,DecisionStatus.DATASET)
          
    val (m,trees,miss) = params        
    val (names,types)  = Features.get(uid)
    
    val model = RF.train(dataset,names.toArray,types.toArray, miss, m, trees)

    val now = new Date()
    val dir = base + "/rf-" + now.getTime().toString
    /* Save model in directory of file system */
    model.save(dir)
    
    /* Put directory to cache for later requests */
    RedisCache.addForest(uid,dir)
          
    /* Update cache */
    RedisCache.addStatus(uid,task,DecisionStatus.FINISHED)
    
  }
  
  private def properties(req:ServiceRequest):(Int,Int,String) = {
      
    try {
      
      val m = req.data("num_selected_features").toInt
      val trees = req.data("trees_per_node").toInt

      val miss = req.data("miss_valu")
        
      return (m,trees,miss)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,DecisionStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.MODEL_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,DecisionStatus.STARTED)	
  
    }

  }
  
}