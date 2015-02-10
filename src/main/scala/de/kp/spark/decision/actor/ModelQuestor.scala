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
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.redis.RedisDB

import de.kp.spark.decision.model._
import de.kp.spark.decision.tree._

class ModelQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  private val redis = new RedisDB(host,port.toInt)
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)

      /*
       * This request retrieves a set of features and computes
       * the target (or decision) variable 
       */
      val resp = if (redis.modelExists(req) == false) {           
        failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
      } else {    
            
         val store = redis.model(req)
         if (store == null) {
           failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
         } else {
              
           try {
              
             val algorithm = req.data(Names.REQ_ALGORITHM)
             val features = Vectors.dense(req.data(Names.REQ_FEATURES).split(",").map(_.toDouble))
             
             algorithm match {
               
               case Algorithms.DT => {
                 
                 val (model,accuracy,categorical_info,num_classes,params) = DTUtil.readDTModel(store)
                 val decision = model.predict(features)
                 
                 new ServiceResponse(req.service,req.task,req.data,DecisionStatus.SUCCESS)       
                 
               }               
               case Algorithms.GBT => {
                 
                 val (model,accuracy,num_classes,params) = GBTUtil.readModel(store)
                 val decision = model.predict(features)
                 
                 new ServiceResponse(req.service,req.task,req.data,DecisionStatus.SUCCESS)       
                 
                 
               }                 
               case Algorithms.RF => {
                 
                 val (model,accuracy,categorical_info,num_classes,params) = DTUtil.readRFModel(store)
                 val decision = model.predict(features)
                 
                 new ServiceResponse(req.service,req.task,req.data,DecisionStatus.SUCCESS)       
                 
               }
                 
               case _ => throw new Exception("Algorithms is not supported.")
               
             }
                
           } catch {
             case e:Exception => failure(req,e.toString())                   

           }
         
         }
      
      }
             
      origin ! resp
      context.stop(self)
    
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
    
  }
  
}