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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.decision.Configuration
import de.kp.spark.decision.source.DecisionSource

import de.kp.spark.decision.model._
import de.kp.spark.decision.tree.RF

import de.kp.spark.decision.sink.RedisSink
import de.kp.spark.decision.util.Fields

import scala.collection.mutable.ArrayBuffer

class RFActor(@transient val sc:SparkContext) extends BaseActor {
  
  private val (base,info) = Configuration.tree
  private val sink = new RedisSink()
  
  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)
      val missing = (params == null)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
 
        try {

          val source = new DecisionSource(sc)
          val dataset = source.get(req)

          if (dataset != null) buildForest(req,dataset,params) else null
          
        } catch {
          case e:Exception => cache.addStatus(req,DecisionStatus.FAILURE)          
        }
 
      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def buildForest(req:ServiceRequest,dataset:RDD[Instance],params:(Int,Int,String)) {
    
    /**
     * The training request must provide a name for the random forest 
     * to uniquely distinguish this forest from all others
     */
    val name = if (req.data.contains(Names.REQ_NAME)) req.data(Names.REQ_NAME) 
      else throw new Exception("No name for decision trees provided.")

    /* Register status */
    cache.addStatus(req,DecisionStatus.MODEL_TRAINING_STARTED)
          
    val (m,trees,miss) = params        
    val (names,types)  = Fields.get(req)
    /*
     * The RF object requires a metadata specification for the features without
     * the target description; note, that the target is the head of the fields
     */
    val model = RF.train(dataset,names.tail.toArray,types.tail.toArray, miss, m, trees)

    val now = new Date()
    val dir = String.format("""%s/%s/%s/%s""",base,name,now.getTime().toString)
    
    /* Save model in directory of file system */
    model.save(dir)
    
    /* Put directory to sink for later requests */
    sink.addForest(req,dir)
          
    /* Update cache */
    cache.addStatus(req,DecisionStatus.MODEL_TRAINING_FINISHED)
    
    /* Notify potential listeners */
    notify(req,DecisionStatus.MODEL_TRAINING_FINISHED)
    
  }
  
  /**
   * This private method retrieves the model parameters from the request
   * and also registers these in the Redis cache
   */
  private def properties(req:ServiceRequest):(Int,Int,String) = {
      
    try {
      
      val params = ArrayBuffer.empty[Param]
      
      val m = req.data("num_selected_features").toInt
      params += Param("num_selected_features","integer",req.data("num_selected_features"))
      
      val trees = req.data("trees_per_node").toInt
      params += Param("trees_per_node","integer",req.data("trees_per_node"))

      val miss = req.data("miss_valu")
      params += Param("miss_valu","integer",req.data("miss_valu"))
      
      cache.addParams(req, params.toList)
      return (m,trees,miss)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
}