name := "concepts"

version := "0.1"

organization := "se.sics"

scalaVersion := "2.10.4"

//Managed dependencies

libraryDependencies  ++= Seq(
            "org.scala-lang" % "scala-reflect" % "2.10.4",
            "org.scalatest" %% "scalatest" % "1.9.1" % "test",
            "org.apache.spark" %% "spark-core" % "1.1.1",
            "org.apache.spark" %% "spark-graphx" % "1.1.1",
            "com.github.scopt" %% "scopt" % "3.2.0",
            "com.typesafe.play" %% "play-json" % "2.2.1"
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

// Set initial commands when entering console

initialCommands in console := """
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import se.sics.concepts.core._
import se.sics.concepts.examples._
// Configure and initialize Spark
val conf = new SparkConf()
  .setMaster("local")
  .setAppName("CNCPT Console")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
  .set("spark.shuffle.consolidateFiles", "true")
val sc = new SparkContext(conf)
"""

// JVM Options

fork in run := true

javaOptions in run += "-Xmx4G"
