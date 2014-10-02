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

import akka.actor.{Actor,ActorLogging}

import de.kp.spark.decision.model._
import de.kp.spark.decision.redis.RedisCache

import de.kp.spark.decision.tree.RFModel

class ModelQuestor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {

        case "get:decision" => {
          /*
           * This request retrieves a set of features and computes
           * the target (or decision) variable 
            */
          val resp = if (RedisCache.forestExist(uid) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    
            
            /* Retrieve path to decision forest for 'uid' from cache */
            val forest = RedisCache.forest(uid)
            if (forest == null) {
              failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
            } else {
              
              if (req.data.contains("features")) {
              
                try {
                
                  val model = new RFModel().loadForest(forest)
                  val decision = model.predict(req.data("features").split(","))

                  val data = Map("uid" -> uid, "decision" -> decision)
                  new ServiceResponse(req.service,req.task,data,DecisionStatus.SUCCESS)
                
                } catch {
                  case e:Exception => {
                    failure(req,e.toString())                   
                  }
                }
                
              } else {
                failure(req,Messages.MISSING_FEATURES(uid))
                
              }
            }
          }
             
          origin ! Serializer.serializeResponse(resp)
          
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          origin ! Serializer.serializeResponse(failure(req,msg))
           
        }
      
      }
    
    }
    
    case _ => {}
    
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,DecisionStatus.FAILURE)	
    
  }
  
}