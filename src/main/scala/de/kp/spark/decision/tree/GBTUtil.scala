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
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import java.io.{ObjectInputStream,ObjectOutputStream}

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.hadoop.conf.{Configuration => HadoopConf}

object GBTUtil {
  
  def readModel(store:String):GradientBoostedTreesModel = {

    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)
    
    val ois = new ObjectInputStream(fs.open(new Path(store)))
    val model = ois.readObject().asInstanceOf[GradientBoostedTreesModel]
      
    ois.close()
    
    model
    
  }

  def writeModel(store:String,model:GradientBoostedTreesModel) {
    
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)

    val oos = new ObjectOutputStream(fs.create(new Path(store)))   
    oos.writeObject(model)
    
    oos.close
    
  }

}