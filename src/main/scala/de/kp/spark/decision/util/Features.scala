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

import de.kp.spark.decision.Configuration
import de.kp.spark.decision.redis.RedisCache

import scala.xml._
import scala.collection.mutable.ArrayBuffer

object Features {

  private val (base,file) = Configuration.tree
  
  def get(uid:String):(ArrayBuffer[String],ArrayBuffer[String]) = {
  
    val names = ArrayBuffer.empty[String]
    val types = ArrayBuffer.empty[String]

    try {
          
      val info = if (RedisCache.metaExists(uid)) {      
        XML.load(RedisCache.meta(uid))
    
      } else {
        XML.load(file)
        
      }
  
      for (att <- info \ "attribute") {
        names += att.text
        /* 
         * The type is either 'C' for categorical 
         * or 'N' for numerical features
         */
        types += (att \ "@type").toString     
      }
      
    } catch {
      case e:Exception => {}
    }
    
    (names,types)
    
  }

}