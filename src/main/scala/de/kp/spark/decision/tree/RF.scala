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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd._
import scala.util.Random

import scala.xml._
import de.kp.spark.decision.model._

/*
 * only a structure used when choosing classification attribute
 */
class AttributeInfo(
  var gainRatio: Double,
  var attributeValues: Array[String],
  var indice: Int=0
  ) {

  def setIndice(indice: Int) {
    this.indice = indice
  }

}
/*
 * The core RF algorithm is similar to Mahout's decision forest
 */
class RF(
  /* missing value indicator (default = ?) */
  val missValue:String,
  /* names of features */
  val names:Array[String],
  /* types of the features */
  val types: Array[String],
  /* number of trees to be build per worker (spark node) */
  val numTree: Int,
  /* dimension of the features (total nuber) */
  val M: Int,
  /* number of selected features for each tree */
  val m: Int,
  /*
   * m selected features' indexes 
   */
  var indexes: Array[Int] = null) extends Serializable {

  /*
   * A helper method to select a randomized subset (m)
   * of all features indexes (0 until M)
   */
  private def randIndexes(m: Int, M: Int): Array[Int] = {
    
    if (m < M && m > 0) {
    
      var result = Array.fill(m)(0)
      var i = 0

      while (i < m) {
        val n = Random.nextInt(M)
        if (!result.contains(n)) {
          result(i) = n
          i += 1
        }
      }
    
      result
    
    } else {
      var result = Array.fill(M)(0)
      for (i <- 0 until M) {
        result(i) = i
      }
      
      result
    
    }
  
  }
  
  /**
   * A helper method to calculate the entropy 
   * of an array of Strings
   */
  private def entropy(buf:Array[String]): Double = {
    
    val len = buf.length
    val invLog2 = 1.0 / Math.log(2)
    if (len > 1) {
      val invLen = 1.0 / len.toDouble
      var ent = 0.0

      for (v <- buf.distinct) {
        
        val p_v = buf.count(x => x == v).toDouble * invLen
        ent -= p_v * Math.log(p_v) * invLog2
      }
      ent

    } else {
      0.0
    }
  }

  /**
   * Bootstrap sampling and select m features
   */
  def baggingAndSelectFeatures(data: Array[Instance]):Array[Instance] = {

    val len = data.length

    val result = for (i <- 0 until len) yield {
      val point = data(Random.nextInt(len))
      var selectedFeatures = Array.fill(m)("")
      for (j <- 0 until m) {
        selectedFeatures(j) = point.features(indexes(j))
      }
      Instance(point.label, selectedFeatures)
    }
    result.toArray
  }

  /**
   * A helper method to calculate the entropy gain 
   * ratio of a numerical attribute 
   */
  private def gainRatioNumerical(att_cat: Array[(String, String)], ent_cat: Double): AttributeInfo = {

    val sorted = att_cat.map(line => (line._1.toDouble, line._2)).sortBy(_._1)
    
    val catValues = sorted.map(line => line._2)
    val attValues = sorted.map(line => line._1)
    
    if (attValues.distinct.length == 1) {
      new AttributeInfo(0, Array(""))
    
    } else {
      
      val invLog2 = 1.0 / math.log(2)
      val len = catValues.length
      
      val invLen = 1.0 / len.toDouble
      var maxInfoGain = 0.0
      var c = 1
      for (i <- 1 until len) {

        if (catValues(i - 1) != catValues(i)) {

          var infoGain = ent_cat
          infoGain -= i * invLen * entropy(catValues.take(i))
          infoGain -= (1 - i * invLen) * entropy(catValues.takeRight(len - i))

          if (infoGain > maxInfoGain) {
            maxInfoGain = infoGain
            c = i
          }
        }
      }
      if (attValues(c) == attValues.last) {
        new AttributeInfo(0, Array(""))
      } else {
        val p = c * invLen
        val ent_att = -p * math.log(p) * invLog2 - (1 - p) * math.log(1 - p) * invLog2
        val infoGainRatio = maxInfoGain / ent_att
        val splitPoint = (attValues(c - 1) + attValues(c)) * 0.5
        new AttributeInfo(infoGainRatio, Array(splitPoint.toString))
      }
    }
  }
  
 /**
  * A helper method to calculate the entropy gain 
  * ratio of a categorical attribute 
  */
  private def gainRatioCategorical(att_cat: Array[(String, String)], ent_cat: Double): AttributeInfo = {
    val att = att_cat.map(_._1)
    val values = att.distinct
    if (values.length != 1) {
      var gain = ent_cat
      val invL = 1.0 / att_cat.length
      for (i <- values) {
        val cat_i = att_cat.filter(_._1 == i).map(_._2)
        gain -= cat_i.length * invL * entropy(cat_i)
      }

      val gainRatio = gain / entropy(att)
      new AttributeInfo(gainRatio, values)
    } else {
      new AttributeInfo(0, values)
    }
  }
  
  /**
   * A helper method to calculate the majority in an array
   */
  def majority(buf: Array[String]): (String, Double) = {
    var major = buf(0)
    var majorityNum = 0
    for (i <- buf.distinct) {
      val tmp = buf.count(x => x == i)
      if (tmp > majorityNum) {
        majorityNum = tmp
        major = i
      }
    }
    (major, majorityNum.toDouble / buf.length)
  }
  
  /**
   * The core function to grow an un-pruning tree
   */
  def growTree(data: Array[Instance], branch: String = ""): Elem = {

    var attList = List[AttributeInfo]()
    
    /*
     * Retrieve all labels from the dataset and compute the entropy
     * of this list
     */    
    val cat = data.map(_.label)
    val ent_cat = entropy(cat)

    if (ent_cat == 0) {
      
      /* specification of a leaf */
      
      <node branch={ branch } type="L">
        <decision p="1">{ data(0).label }</decision>
      </node>
    } else {
      
      /*
       * Entropy of the labels is not zero
       */      
      for (i <- 0 until m) {
        
        val att_cat = data.filter(_.features(i) != missValue)
          .map(obs => (obs.features(i), obs.label))
        
          if (att_cat.length > 0) {
          
          val fid = indexes(i)
          if (types(fid) =="N") {
            /* numerical feature */
            val attInfo = gainRatioNumerical(att_cat, ent_cat)
            attInfo.setIndice(i)
            attList ::= attInfo
          
          } else {
            /* categorical feature */
            val attInfo = gainRatioCategorical(att_cat, ent_cat)
            attInfo.setIndice(i)
            attList ::= attInfo

          }
        }
      }
      /*
       * find the best attribute for classification
       */
      val chosen = attList.maxBy(_.gainRatio)
      if (chosen.gainRatio == 0) {
        val (decision, p) = majority(data.map(_.label))
        <node branch={ branch } type="L">
          <decision p={ p.toString }>{ decision }</decision>
        </node>

      } else {
        var priorDecision = ""
        val fid = indexes(chosen.indice)
        if (types(fid) == "N") {
          /* numerical attribute */
          val splitPoint = chosen.attributeValues(0).toDouble
          val lochilddata = data.filter { obs =>
            val j = obs.features(chosen.indice)
            j != missValue && j.toDouble <= splitPoint
          }

          val hichilddata = data.filter { obs =>
            val j = obs.features(chosen.indice)
            j != missValue && j.toDouble > splitPoint
          }
          var p = lochilddata.length.toDouble / data.length
          if (p <= 0.5) {
            p = 1 - p
            priorDecision = "high"
          } else {
            priorDecision = "low"
          }

          <node branch={ branch } 
                type="N" 
                name={ names(fid) } 
                indice={ fid.toString } 
                splitPoint={ splitPoint.toString }>
            { growTree(lochilddata, "low") }
            { growTree(hichilddata, "high") }
            <priorDecision p={ p.toString }>{
              priorDecision
            }</priorDecision>
          </node>

        } else {
          
          /* categorical attribute */
          var majorityNum = 0

          <node branch={ branch } 
                type="C" 
                name={ names(fid) } 
                indice={ fid.toString }>
            {
              for (i <- chosen.attributeValues) yield {
                val child = data.filter { obs =>
                  val j = obs.features(chosen.indice)
                  j != missValue && j == i
                }
                if (child.length > majorityNum) {
                  majorityNum = child.length
                  priorDecision = i
                }
                growTree(child, i)
              }
            }
            <priorDecision p={ (majorityNum.toDouble / data.length).toString }>{
              priorDecision
            }</priorDecision>
          </node>

        }
      }

    }

  }

  def run(data: RDD[Instance]): RFModel = {

    var total_len = data.count    
    var forest = data.mapPartitionsWithIndex { (index, obs) =>

      val partial = obs.toArray
      val treesPerWorker = for (i <- 1 to numTree) yield {
        
        indexes = randIndexes(m, M)
        val oob = baggingAndSelectFeatures(partial)

        growTree(oob)

      }
    
      treesPerWorker.iterator
    
    }.collect

    new RFModel(forest, missValue)
    
  }
  
}

object RF {

  def train(
    /* The features that have to be evaluated */
    data:RDD[Instance],
    /* The metadata for the features */
    names:Array[String],
    types:Array[String],
    /* Indicator for a missing value */
    missValue:String,
    /* The number of features that have to be selected */
    m:Int,
    /* The number of trees per worker (thread) */
    numTree:Int):RFModel = {
      
      /*
       * M is the number of features, and 'm' is the number of 
       * selected feature to train the model (m is usually a 
       * subset of all features); the user should take care, 
       * that m <= M
       */
      val M = names.length
      if (m < M) {
        new RF(missValue,names,types,numTree,M,m).run(data)
      
      } else {
        
        new RF(missValue,names,types,numTree,M,M).run(data)
      }
    }

}