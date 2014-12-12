package de.kp.spark.decision.actor
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
import akka.actor.{ActorRef,Props}

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.decision.Configuration

class DecisionMaster(@transient sc:SparkContext) extends BaseMaster(Configuration) {
   
  protected def actor(worker:String):ActorRef = {
    
    worker match {
      /*
       * Metadata management is part of the core functionality; field or metadata
       * specifications can be registered in, and retrieved from a Redis database.
       */
      case "fields"   => context.actorOf(Props(new FieldQuestor(Configuration)))
      case "register" => context.actorOf(Props(new DecisionRegistrar()))  
      /*
       * Index management is part of the core functionality; an Elasticsearch 
       * index can be created and appropriate (tracked) items can be saved.
       */  
      case "index" => context.actorOf(Props(new BaseIndexer(Configuration)))        
      case "track" => context.actorOf(Props(new BaseTracker(Configuration)))
        
      case "params" => context.actorOf(Props(new ParamQuestor(Configuration)))        
      case "status" => context.actorOf(Props(new StatusQuestor(Configuration)))        
      
      case "get"   => context.actorOf(Props(new ModelQuestor()))
      case "train" => context.actorOf(Props(new ModelBuilder(sc)))  
      
      case _ => null
      
    }
  
  }

}