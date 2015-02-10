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

import de.kp.spark.core.model._

object Serializer extends BaseSerializer 

object Algorithms {
    
  val DT:String  = "DT"
  val GBT:String = "GBT"
  val RF:String  = "RF"
  
  private val algorithms = List(DT,GBT,RF)
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
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