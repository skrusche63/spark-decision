package de.kp.spark.decision.tree
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
import org.apache.spark.mllib.tree.model.{DecisionTreeModel,RandomForestModel}

import java.io.{ObjectInputStream,ObjectOutputStream}

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.hadoop.conf.{Configuration => HadoopConf}

private class DTStruct(
  val model:DecisionTreeModel,
  val accuracy:Double,
  val categorical_info:Map[Int,Int],
  val num_classes:Int,
  val params:Map[String,String]
) extends Serializable {}

private class RFStruct(
  val model:RandomForestModel,
  val accuracy:Double,
  val categorical_info:Map[Int,Int],
  val num_classes:Int,
  val params:Map[String,String]
) extends Serializable {}

object DTUtil {
  
  def readDTModel(store:String):(DecisionTreeModel,Double,Map[Int,Int],Int,Map[String,String]) = {

    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)
    
    val ois = new ObjectInputStream(fs.open(new Path(store)))
    val struct = ois.readObject().asInstanceOf[DTStruct]
      
    ois.close()
    
    (struct.model,struct.accuracy,struct.categorical_info,struct.num_classes,struct.params)
    
  }
  
  def readRFModel(store:String):(RandomForestModel,Double,Map[Int,Int],Int,Map[String,String]) = {

    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)
    
    val ois = new ObjectInputStream(fs.open(new Path(store)))
    val struct = ois.readObject().asInstanceOf[RFStruct]
      
    ois.close()
    
    (struct.model,struct.accuracy,struct.categorical_info,struct.num_classes,struct.params)
    
  }

  def writeDTModel(store:String,model:DecisionTreeModel,accuracy:Double,categorical_info:Map[Int,Int],num_classes:Int,params:Map[String,String]) {
    
    val struct = new DTStruct(model,accuracy,categorical_info,num_classes,params)
    
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)

    val oos = new ObjectOutputStream(fs.create(new Path(store)))   
    oos.writeObject(struct)
    
    oos.close
    
  }

  def writeRFModel(store:String,model:RandomForestModel,accuracy:Double,categorical_info:Map[Int,Int],num_classes:Int,params:Map[String,String]) {
    
    val struct = new RFStruct(model,accuracy,categorical_info,num_classes,params)
    
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)

    val oos = new ObjectOutputStream(fs.create(new Path(store)))   
    oos.writeObject(struct)
    
    oos.close
    
  }

}