package de.kp.spark.decision.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.core.model._

case class Instance(label: String, features: Array[String]) {

  override def toString: String = {
    "Observation(%s, %s)".format(label, features.mkString("[", ", ", "]"))
  }

}

object Serializer extends BaseSerializer 

object Algorithms {
  /* The value of the algorithms actually supported */
  val RF:String = "RF"
  
  private val algorithms = List(RF)
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}

object Sources {
  /* The names of the data source actually supported */
  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"   
  val PARQUET:String = "PARQUET"

  private val sources = List(ELASTIC,FILE,JDBC,PARQUET)
  def isSource(source:String):Boolean = sources.contains(source)
    
}

object Messages extends BaseMessages {
  
  def MISSING_FEATURES(uid:String):String = 
    String.format("""[UID: %s] Features are missing.""", uid)

  def MISSING_PARAMETERS(uid:String):String = 
    String.format("""[UID: %s] Parameters are missing.""", uid)

  def MODEL_BUILDING_STARTED(uid:String) = 
    String.format("""[UID: %s] Top-K Association Rule Mining started.""", uid)

  def MODEL_DOES_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] Model does not exist.""", uid)
  
}

object DecisionStatus extends BaseStatus