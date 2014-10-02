package de.kp.spark.decision.actor

import akka.actor.Actor
import org.apache.spark.rdd.RDD

import de.kp.spark.decision.Configuration

//import de.kp.spark.decision.source.TransactionSource
import de.kp.spark.decision.model._

import de.kp.spark.decision.redis.RedisCache

class RFActor extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("TopKActor",Configuration.spark)      
  
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

          // TODO
          
        } catch {
          case e:Exception => RedisCache.addStatus(uid,task,DecisionStatus.FAILURE)          
        }
 
      }
      
      sc.stop
      context.stop(self)
          
    }
    
    case _ => {
      
      sc.stop
      context.stop(self)
      
    }
    
  }
  
  private def properties(req:ServiceRequest):(Int,Int) = {
      
    try {
      
      val selected = req.data("num_selected_features").toInt
      val trees = req.data("trees_per_node").toInt
        
      return (selected,trees)
        
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