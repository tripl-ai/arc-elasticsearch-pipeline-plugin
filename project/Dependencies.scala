import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "2.4.3"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "2.0.0" % "provided"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()  

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" 

  // elasticsearch
  val elasticsearch = "org.elasticsearch" % "elasticsearch-hadoop" % "7.0.1" intransitive()

  // Project
  val etlDeps = Seq(
    scalaTest,
    
    arc,
    typesafeConfig,

    sparkSql,
    sparkHive,

    elasticsearch
  )
}