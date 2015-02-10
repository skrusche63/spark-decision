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

import org.apache.spark.mllib.tree.{DecisionTree,RandomForest,impurity}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel,RandomForestModel}

import org.apache.spark.mllib.tree.configuration.{Algo,Strategy}

object DT extends Serializable {
  
  def trainTree(dataset:RDD[LabeledPoint],categorical_info:Map[Int,Int],num_classes:Int,params:Map[String,String]):(DecisionTreeModel,Double) = {

    val algo_type = if (params.contains("algorithm_type")) params("algorithm_type") else ""
    if (algo_type == "") new Exception("Algorithm type is not provided.")
    
    val algo_types = List("Classification","Regression")
    if (algo_types.contains(algo_type) == false) throw new Exception("Algorithm type is not supported.")

    val num_trees = params("num_trees").toInt
    require(num_trees == 1)

    /*
     * Maximum number of bins used for discretizing continuous features and
     * for choosing how to split on features at each node. More bins give higher 
     * granularity.
     */
    val max_bins = if (params.contains("max_bins")) params("max_bins").toInt else 32

    /* Criterion used for information gain calculation */
    val impurity_type = if (params.contains("impurity_type")) params("impurity_type") else ""
    if (impurity_type == "") new Exception("Impurity is not provided.")
      
    val impurity_calc = impurity_type match {
      /* CLASSIFICATION */
      case "GINI"     => impurity.Gini
      case "ENTROPY"  => impurity.Entropy
      /* REGRESSION */
      case "VARIANCE" => impurity.Variance
      
    }
    
    /*
     * Maximum depth of the tree. E.g., depth 0 means 1 leaf node; 
     * depth 1 means 1 internal node + 2 leaf nodes.
     */
    val max_depth = if (params.contains("max_depth")) params("max_depth").toInt else 5
    
    val strategy = new Strategy(
       algo = if (algo_type == "Classification") Algo.Classification else Algo.Regression,
       impurity = impurity_calc,
       maxDepth = max_depth,
       maxBins = max_bins,
       numClasses = num_classes)
      
    /*
     * A map storing information about the categorical variables and the
     * number of discrete values they take. 
     * 
     * For example, an entry (n -> k) implies the feature n is categorical 
     * with k categories 0, 1, 2, ... , k-1. 
     * 
     * It's important to note that features are zero-indexed.
     */
    if (categorical_info.empty == false)
      strategy.setCategoricalFeaturesInfo(categorical_info)

    val model = DecisionTree.train(dataset,strategy)
    val accuracy = if (algo_type == "Classification") {
      new MulticlassMetrics(dataset.map(x => (model.predict(x.features), x.label))).precision

    } else {
      dataset.map(x => {
        val err = model.predict(x.features) - x.label
        err * err
      }).mean()
 
    }
    
    (model,accuracy)
    
  }

  def trainForest(dataset:RDD[LabeledPoint],categorical_info:Map[Int,Int],num_classes:Int,params:Map[String,String]):(RandomForestModel,Double) = {

    val algo_type = if (params.contains("algorithm_type")) params("algorithm_type") else ""
    if (algo_type == "") new Exception("Algorithm type is not provided.")
    
    val algo_types = List("Classification","Regression")
    if (algo_types.contains(algo_type) == false) throw new Exception("Algorithm type is not supported.")

    val num_trees = params("num_trees").toInt
    require(num_trees > 1)
    /*
     * Maximum number of bins used for discretizing continuous features and
     * for choosing how to split on features at each node. More bins give higher 
     * granularity.
     */
    val max_bins = if (params.contains("max_bins")) params("max_bins").toInt else 32

    /* Criterion used for information gain calculation */
    val impurity_type = if (params.contains("impurity_type")) params("impurity_type") else ""
    if (impurity_type == "") new Exception("Impurity is not provided.")
      
    val impurity_calc = impurity_type match {
      /* CLASSIFICATION */
      case "GINI"     => impurity.Gini
      case "ENTROPY"  => impurity.Entropy
      /* REGRESSION */
      case "VARIANCE" => impurity.Variance
      
    }
    
    /*
     * Maximum depth of the tree. E.g., depth 0 means 1 leaf node; 
     * depth 1 means 1 internal node + 2 leaf nodes.
     */
    val max_depth = if (params.contains("max_depth")) params("max_depth").toInt else 5
    
    /*
     * Number of features to consider for splits at each node.
     * Supported: "auto", "all", "sqrt", "log2", "onethird".
     * 
     * If "auto" is set, this parameter is set based on num_tree:
     * if numTrees == 1, set to "all";
     * if numTrees > 1 (forest) set to "sqrt".
     */
    val feature_subset_strategy = "all"
      
    val strategy = new Strategy(
       algo = if (algo_type == "Classification") Algo.Classification else Algo.Regression,
       impurity = impurity_calc,
       maxDepth = max_depth,
       maxBins = max_bins,
       numClasses = num_classes)
      
    /*
     * A map storing information about the categorical variables and the
     * number of discrete values they take. 
     * 
     * For example, an entry (n -> k) implies the feature n is categorical 
     * with k categories 0, 1, 2, ... , k-1. 
     * 
     * It's important to note that features are zero-indexed.
     */
    if (categorical_info.empty == false)
      strategy.setCategoricalFeaturesInfo(categorical_info)
    
    /*
     * Random seed for bootstrapping and choosing feature subsets.
     */
    val random_seed = new scala.util.Random().nextInt()
    if (algo_type == "Classification") {
        
      val model = RandomForest.trainClassifier(dataset,strategy,num_trees,feature_subset_strategy,random_seed)
      val accuracy = new MulticlassMetrics(dataset.map(x => (model.predict(x.features), x.label))).precision

      (model, accuracy)
      
    } else {

      val model = RandomForest.trainRegressor(dataset,strategy,num_trees,feature_subset_strategy, random_seed)     
      val accuracy = dataset.map(x => {
        val err = model.predict(x.features) - x.label
        err * err
      }).mean()

      (model, accuracy)

    }
    
  }

}