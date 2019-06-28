package ai.tripl.arc.load

import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.typesafe.config._

import org.elasticsearch.spark.sql._ 

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

class ElasticsearchLoad extends PipelineStagePlugin {

  val version = ai.tripl.arc.elasticsearch.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments"  :: "inputView" :: "output"  :: "numPartitions" :: "partitionBy" :: "saveMode" :: "persist" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val output = getValue[String]("output")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    (name, description, inputView, output, persist, numPartitions, partitionBy, saveMode, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(output), Right(persist), Right(numPartitions), Right(partitionBy), Right(saveMode), Right(invalidKeys)) => 

        val stage = ElasticsearchLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          output=output,
          params=params,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          saveMode=saveMode
        )

        stage.stageDetail.put("inputView", inputView)  
        stage.stageDetail.put("output", output)  
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, output, persist, numPartitions, partitionBy, saveMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class ElasticsearchLoadStage(
    plugin: ElasticsearchLoad,
    name: String, 
    description: Option[String], 
    inputView: String, 
    output: String, 
    partitionBy: List[String], 
    numPartitions: Option[Int], 
    saveMode: SaveMode, 
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    ElasticsearchLoadStage.execute(this)
  }
}

object ElasticsearchLoadStage {

  def execute(stage: ElasticsearchLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {

    val df = spark.table(stage.inputView)      

    stage.numPartitions match {
      case Some(partitions) => stage.stageDetail.put("numPartitions", Integer.valueOf(partitions))
      case None => stage.stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
    }

    val dropMap = new java.util.HashMap[String, Object]()

    // elasticsearch cannot support a column called _index
    val unsupported = df.schema.filter( _.name == "_index").map(_.name)
    if (!unsupported.isEmpty) {
      dropMap.put("Unsupported", unsupported.asJava)
    }

    stage.stageDetail.put("drop", dropMap)    
    
    val nonNullDF = df.drop(unsupported:_*)

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    // Elasticsearch will convert date and times to epoch milliseconds
    val outputDF = try {
      stage.partitionBy match {
        case Nil =>
          val dfToWrite = stage.numPartitions.map(nonNullDF.repartition(_)).getOrElse(nonNullDF)
          dfToWrite.write.options(stage.params).mode(stage.saveMode).format("org.elasticsearch.spark.sql").save(stage.output)
          dfToWrite
        case partitionBy => {
          // create a column array for repartitioning
          val partitionCols = partitionBy.map(col => nonNullDF(col))
          stage.numPartitions match {
            case Some(n) =>
              val dfToWrite = nonNullDF.repartition(n, partitionCols:_*)
              dfToWrite.write.options(stage.params).partitionBy(partitionBy:_*).mode(stage.saveMode).format("org.elasticsearch.spark.sql").save(stage.output)
              dfToWrite
            case None =>
              val dfToWrite = nonNullDF.repartition(partitionCols:_*)
              dfToWrite.write.options(stage.params).partitionBy(partitionBy:_*).mode(stage.saveMode).format("org.elasticsearch.spark.sql").save(stage.output)
              dfToWrite
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    spark.sparkContext.removeSparkListener(listener)

    Option(outputDF)
  }
}