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

import org.apache.spark.mllib.regression

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.redis.RedisDB

import de.kp.spark.core.source.PointSource
import de.kp.spark.core.source.handler.LabeledPointHandler

import de.kp.spark.decision.Configuration

import de.kp.spark.decision.model._
import de.kp.spark.decision.tree.{DT,DTHandler,DTUtil}

import de.kp.spark.decision.spec.PointSpec
import scala.collection.mutable.ArrayBuffer

class DTActor(@transient val sc:SparkContext) extends BaseActor {
  
  private val config = Configuration
  private val (base,info) = config.tree
  
  private val redis = new RedisDB(host,port.toInt)
  
  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)
      val missing = (params == null)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
 
        try {

          val source = new PointSource(sc,config,new PointSpec(req))
          val (trainset,categorical_info,num_classes) = DTHandler.format(source.connect(req))

          train(req,trainset,categorical_info,num_classes,params)
          
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
  private def train(req:ServiceRequest,dataset:RDD[regression.LabeledPoint],categorical_info:Map[Int,Int],num_classes:Long,params:Map[String,String]) {
    
    /**
     * The training request must provide a name for the Gradient Boosted 
     * Trees to uniquely distinguish these trees from all other ones
     */
    val name = if (req.data.contains(Names.REQ_NAME)) req.data(Names.REQ_NAME) 
      else throw new Exception("No name for gradient boosted trees provided.")

    /* Register status */
    cache.addStatus(req,DecisionStatus.MODEL_TRAINING_STARTED)
    
    val num_trees = req.data("num_trees").toInt
    if (num_trees == 1) {

      val (model,accuracy) = DT.trainTree(dataset,categorical_info,num_classes.toInt,params)

      val now = new java.util.Date()
      val store = String.format("""%s/%s/%s/%s""",base,name,now.getTime().toString)
    
      /* Save model in directory of file system */
      DTUtil.writeDTModel(store, model, accuracy, categorical_info,num_classes.toInt,params)
    
      /* Put directory to sink for later requests */
      redis.addModel(req,store)
      
    } else if (num_trees > 1) {

      val (model,accuracy) = DT.trainForest(dataset,categorical_info,num_classes.toInt,params)

      val now = new java.util.Date()
      val store = String.format("""%s/%s/%s/%s""",base,name,now.getTime().toString)
    
      /* Save model in directory of file system */
      DTUtil.writeRFModel(store, model, accuracy, categorical_info,num_classes.toInt,params)
    
      /* Put directory to sink for later requests */
      redis.addModel(req,store)
      
    } else {
      throw new Exception("Illegal number of trees provided.")
    
    }
    
    /* Update cache */
    cache.addStatus(req,DecisionStatus.MODEL_TRAINING_FINISHED)
    
  }
  
  /**
   * This private method retrieves the model parameters from the request
   * and also registers these in the Redis cache
   */
  private def properties(req:ServiceRequest):Map[String,String] = {
      
    try {
      
      val params = ArrayBuffer.empty[Param]

      val algo_type = req.data("algorithm_type")
      params += Param("algorithm_type","string",req.data("algorithm_type"))
    
      val algo_types = List("Classification","Regression")
      if (algo_types.contains(algo_type) == false) throw new Exception("Algorithm type is not supported.")

      /*
       * Maximum depth of the tree. E.g., depth 0 means 1 leaf node; 
       * depth 1 means 1 internal node + 2 leaf nodes.
       */
      val max_depth = if (req.data.contains("max_depth")) req.data("max_depth").toInt else 5
      params += Param("max_depth","integer",req.data("max_depth"))
      /*
       * Maximum number of bins used for discretizing continuous features and
       * for choosing how to split on features at each node. More bins give higher 
       * granularity.
       */
      val max_bins = if (req.data.contains("max_bins")) req.data("max_bins").toInt else 32
      params += Param("max_bins","integer",req.data("max_bins"))

      /* Criterion used for information gain calculation */
      val impurity_type = req.data("impurity_type")
      params += Param("impurity_type","string",req.data("impurity_type"))

      val num_trees = req.data("num_trees").toInt
      params += Param("num_trees","integer",req.data("num_trees"))

      cache.addParams(req, params.toList)
      req.data
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }

}