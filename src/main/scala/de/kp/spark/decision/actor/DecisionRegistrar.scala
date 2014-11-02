package de.kp.spark.decision.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Spark-Decision project
 * (https://github.com/skrusche63/spark-fm).
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
import de.kp.spark.decision.redis.RedisCache

import scala.collection.mutable.ArrayBuffer

class DecisionRegistrar extends BaseActor {
  
  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")
      
      val response = try {
        
        /* Unpack fields from request and register in Redis instance */
        val fields = ArrayBuffer.empty[Field]

        /*
         * ********************************************
         * 
         *  "uid" -> 123
         *  "names" -> "target,feature,feature,feature"
         *  "types" -> "string,double,double,string"
         *
         * ********************************************
         * 
         * It is important to have the names specified in the order
         * they are used (later) to retrieve the respective data
         */
        val names = req.data("names").split(",")
        val types = req.data("types").split(",")
        
        val zip = names.zip(types)
        
        val target = zip.head
        if (target._2 != "string") throw new Exception("Target variable must be a String")
        
        fields += new Field(target._1,target._2,"")
        
        for (feature <- zip.tail) {
          
          if (feature._2 != "string" && feature._2 != "double") throw new Exception("A feature must either be a String or a Double.")          
          fields += new Field(feature._1, if (feature._2 == "string") "C" else "N","")
        
        }
 
        RedisCache.addFields(req, new Fields(fields.toList))
        
        new ServiceResponse("decision","register",Map("uid"-> uid),DecisionStatus.SUCCESS)
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! Serializer.serializeResponse(response)

    }
    
  }

}