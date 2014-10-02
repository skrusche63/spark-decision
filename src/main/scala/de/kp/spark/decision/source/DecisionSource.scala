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

class DecisionSource(@transient sc:SparkContext) {
  
  def get(data:Map[String,String]):RDD[Instance] = {
    
    val source = data("source")
    source match {
      /* 
       * Build decision model from features persisted as an appropriate 
       * search index from Elasticsearch; the configuration parameters 
       * are retrieved from the service configuration 
       */    
      case Sources.ELASTIC => new ElasticSource(sc).connect(data)
      /* 
       * Build decision model from features persisted as a file on the 
       * (HDFS) file system; the configuration parameters are retrieved 
       * from the service configuration  
       */    
      case Sources.FILE => new FileSource(sc).connect(data)
      /* 
       * Build decision model from features persisted as an appropriate 
       * table from a JDB database; the configuration parameters are 
       * retrieved from the service configuration 
       * 
       */
      case Sources.JDBC => new JdbcSource(sc).connect(data)
            
      case _ => null
      
    }
    
  }

}