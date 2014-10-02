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

import scala.xml._
import scala.collection.mutable.ArrayBuffer

object FeatureSpec {

  private val path = Configuration.tree
  
  private val names = ArrayBuffer.empty[String]
  private val types = ArrayBuffer.empty[String]
  
  private val info = XML.load(path)
  
  /*
   * Read the XML (info file) to extract the
   * feature names and also their types
   */
  for (att <- info \ "attribute") {
     
    names += att.text
    /* 
     * The type is either 'C' for categorical or 'N'
     * for numerical features
     */
    types += (att \ "@type").toString
     
  }

  def get() = (names,types)
  
}