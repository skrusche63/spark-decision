package de.kp.spark.decision
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

import akka.actor.{ActorSystem,Props}
import com.typesafe.config.ConfigFactory

import de.kp.spark.core.SparkService
import de.kp.spark.decision.actor.DecisionMaster

object DecisionService {

  def main(args: Array[String]) {
    
    val name:String = "decision-server"
    val conf:String = "server.conf"

    val server = new DecisionService(conf, name)
    while (true) {}
    
    server.shutdown
      
  }

}

class DecisionService(conf:String, name:String) extends SparkService {

  val system = ActorSystem(name, ConfigFactory.load(conf))
  sys.addShutdownHook(system.shutdown)
  
  /* Create Spark context */
  private val sc = createCtxLocal("DecisionContext",Configuration.spark)      

  val master = system.actorOf(Props(new DecisionMaster(sc)), name="decision-master")

  def shutdown = system.shutdown()
  
}