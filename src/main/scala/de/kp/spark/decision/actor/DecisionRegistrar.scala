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

import de.kp.spark.core.model._
import de.kp.spark.core.actor.FeatureRegistrar

import de.kp.spark.decision.Configuration
import scala.collection.mutable.ArrayBuffer

class DecisionRegistrar extends FeatureRegistrar(Configuration) {
  
  override def buildFields(names:Array[String],types:Array[String]):Fields = {
        
    val fields = ArrayBuffer.empty[Field]
        
    val zip = names.zip(types)
        
    val target = zip.head
    if (target._2 != "string") throw new Exception("Target variable must be a String")
        
    fields += new Field(target._1,target._2,"")
        
    for (feature <- zip.tail) {
          
      if (feature._2 != "string" && feature._2 != "double") throw new Exception("A feature must either be a String or a Double.")          
      fields += new Field(feature._1, if (feature._2 == "string") "C" else "N","")
        
    }
    
    Fields(fields.toList)
  
  }

}