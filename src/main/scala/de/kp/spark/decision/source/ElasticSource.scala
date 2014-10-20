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

import de.kp.spark.decision.model._
import de.kp.spark.decision.io.ElasticReader

import de.kp.spark.decision.util.Features
import scala.collection.mutable.ArrayBuffer

class ElasticSource(@transient sc:SparkContext) extends Source(sc) {
 
  override def connect(params:Map[String,String]):RDD[Instance] = {
    
    val index = params("index")
    val mapping = params("type")
    
    val query = params("query")
    
    val (names,types) = Features.get(params)    
    val spec = sc.broadcast(names)
    
    /* Connect to Elasticsearch */
    val rawset = new ElasticReader(sc,index,mapping,query).read
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

}