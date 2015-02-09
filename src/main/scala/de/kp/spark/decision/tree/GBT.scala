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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.tree.configuration.BoostingStrategy

object GBT extends Serializable {

  def train(dataset:RDD[LabeledPoint],params:Map[String,String]):(GradientBoostedTreesModel,Double) = {

    val algo_type = if (params.contains("algorithm_type")) params("algorithm_type") else ""
    if (algo_type == "") new Exception("Algorithm type is not provided.")
    
    val algo_types = List("Classification","Regression")
    if (algo_types.contains(algo_type) == false) throw new Exception("Algorithm type is not supported.")
    
    val num_classes = if (params.contains("num_classes")) params("num_classes").toInt else -1
    if (num_classes == -1)  new Exception("Number of classes is not provided.")

    val max_depth = if (params.contains("max_depth")) params("max_depth").toInt else 5
    val max_iterations = if (params.contains("max_iterations")) params("max_iterations").toInt else 20
    
    /*
     * Specify boosting strategy
     */
    val boostingStrategy = BoostingStrategy.defaultParams(algo_type)    
    boostingStrategy.treeStrategy.numClasses = num_classes
    
    boostingStrategy.numIterations = max_iterations
    boostingStrategy.treeStrategy.maxDepth = max_depth

    if (algo_type == "Classification") {
      
      val model = GradientBoostedTrees.train(dataset, boostingStrategy)
      val accuracy = new MulticlassMetrics(dataset.map(x => (model.predict(x.features), x.label))).precision
    
      (model,accuracy)
      
    } else {
 
      val model = GradientBoostedTrees.train(dataset, boostingStrategy)
      val accuracy = dataset.map(x => {
        val err = model.predict(x.features) - x.label
        err * err
      }).mean()
      
      (model,accuracy)

    }
    
  }
  
}