package de.kp.spark.decision.util
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

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisCache

import de.kp.spark.decision.Configuration

import scala.xml._
import scala.collection.mutable.ArrayBuffer

/**
 * Fields object supports feature specifications either
 * dynamically provided and retrieved from the Redis instance,
 * or by specifying a certain template.
 */
object Fields {

  private val (base,file) = Configuration.tree

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)
  
  def get(req:ServiceRequest):(ArrayBuffer[String],ArrayBuffer[String]) = {
  
    val names = ArrayBuffer.empty[String]
    val types = ArrayBuffer.empty[String]

    try {
      /*
       * First we try to retrieve the feature specification from the
       * Redis instance, i.e. dynamic first 
       */    
      if (cache.fieldsExist(req)) {     
        
        val fieldspec = cache.fields(req)
        for (field <- fieldspec) {
          
          names += field.name
          types += field.datatype

        }  
    
      } else {
        /*
         * Future: Distinguish between different templates of
         * feature specification
         */
        val info = XML.load(file)
        for (att <- info \ "attribute") {
          names += att.text
          /* 
           * The type is either 'C' for categorical 
           * or 'N' for numerical features
           */
          types += (att \ "@type").toString     
        }
        
      }
      
    } catch {
      case e:Exception => {}
    }
    
    (names,types)
    
  }

}