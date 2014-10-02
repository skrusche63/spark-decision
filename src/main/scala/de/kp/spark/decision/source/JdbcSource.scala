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

import de.kp.spark.decision.Configuration
import de.kp.spark.decision.model._

class JdbcSource(@transient sc:SparkContext) extends Source(sc) {

  protected val MYSQL_DRIVER   = "com.mysql.jdbc.Driver"
  protected val NUM_PARTITIONS = 1
   
  protected val (url,database,user,password) = Configuration.mysql
  
  override def connect(params:Map[String,Any]):RDD[Instance] = {
    
    /* Retrieve site and query from params */
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]
    
    // TODO
    null
    
  }

}