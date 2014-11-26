package de.kp.spark.decision.source
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

import de.kp.spark.core.model._
import de.kp.spark.decision.model._

import de.kp.spark.decision.util.Fields
import scala.collection.mutable.ArrayBuffer

class DecisionModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[Instance] = {
    
    val (names,types) = Fields.get(req)    
    val spec = sc.broadcast(names)
    
    rawset.map(data => {
      
      val fields = spec.value

      val label = data(fields.head)
      val features = ArrayBuffer.empty[String]
      
      for (field <- fields.tail) {
        features += data(field)
      }
      
      new Instance(label,features.toArray)
      
    })
  }
  
  def buildFile(req:ServiceRequest,rawset:RDD[String]):RDD[Instance] = {
   
    rawset.map {line =>
      /*
       * init() selects all elements except the last ->
       * 
       * last = (classifier) label
       * init = feature vector that is classified with this label
       */
      val parts = line.split(',')
      Instance(parts.last, parts.init)
    
    }
    
  }
  
  def buildJDBC(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[Instance] = {
    
    val (names,types) = Fields.get(req)    
    
    val bcnames = sc.broadcast(names)
    val bctypes = sc.broadcast(types)
    
    rawset.map(data => {
     
      val label = data(bcnames.value.head).asInstanceOf[String]
      val features = ArrayBuffer.empty[String]
      
      for (name <- bcnames.value.tail) {
        
        val ftype = bctypes.value(bcnames.value.indexOf(name))
        val feature = if (ftype == "C") {
          data(name).asInstanceOf[String]
        
        } else {
          data(name).asInstanceOf[Double]
        
        }
        
        features += feature.toString
      }
      
      new Instance(label,features.toArray)
      
    })

  }

}