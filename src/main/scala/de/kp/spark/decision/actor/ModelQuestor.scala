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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.decision.model._
import de.kp.spark.decision.sink.RedisSink

import de.kp.spark.decision.tree.RFModel

class ModelQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  private val sink = new RedisSink()
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)

      /*
       * This request retrieves a set of features and computes
       * the target (or decision) variable 
       */
      val resp = if (sink.forestExists(uid) == false) {           
        failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
      } else {    
            
         /* Retrieve path to decision forest for 'uid' from sink */
         val forest = sink.forest(uid)
         if (forest == null) {
           failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
         } else {
              
            if (req.data.contains(Names.REQ_FEATURES)) {
              
              try {
                
                val model = new RFModel().loadForest(forest)
                val decision = model.predict(req.data(Names.REQ_FEATURES).split(","))

                val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> decision)
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