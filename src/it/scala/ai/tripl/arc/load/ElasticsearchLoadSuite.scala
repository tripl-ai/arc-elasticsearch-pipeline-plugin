package ai.tripl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties

import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.HttpClientBuilder

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._
import org.elasticsearch.spark.sql._ 

class ElasticsearchLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val testData = getClass.getResource("/akc_breed_info.csv").toString
  val inputView = "expected"
  val index = "dogs"
  val esURL = "elasticsearch"
  val port = "9200"
  val wanOnly = "true"
  val ssl = "false"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("es.index.auto.create", "true")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
  }

  after {
    session.stop
  }

  test("ElasticsearchLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df0 = spark.read.option("header","true").csv(testData)
    df0.createOrReplaceTempView(inputView)

    val client = HttpClientBuilder.create.build
    val delete = new HttpDelete(s"http://${esURL}:9200/index")
    val response = client.execute(delete)
    response.close 

    load.ElasticsearchLoadStage.execute(
      load.ElasticsearchLoadStage(
        plugin=new load.ElasticsearchLoad,
        name="df", 
        description=None,
        inputView=inputView, 
        output=index,
        numPartitions=None,
        params=Map("es.nodes.wan.only" -> wanOnly, "es.port" -> port, "es.net.ssl" -> ssl, "es.nodes" -> esURL),
        saveMode=SaveMode.Overwrite,
        partitionBy=Nil
      )
    )   

    val df1 = spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only",wanOnly)
      .option("es.port", port)
      .option("es.net.ssl", ssl)
      .option("es.nodes", esURL)
      .load(index)

    df1.createOrReplaceTempView("actual")


    // reselect fields to ensure correct order
    val expected = spark.sql(s"""
    SELECT Breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM ${inputView}
    """)

    // reselect fields to ensure correct order
    val actual = spark.sql(s"""
    SELECT Breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM actual
    """)

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(100000, false)
      println("expected")
      expected.show(100000, false)  
    }
    assert(actualExceptExpectedCount === 0)
    assert(expectedExceptActualCount === 0)
  } 


  test("ElasticsearchLoad end-to-end") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = spark.read.option("header","true").csv(testData)
    df.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "ElasticsearchLoad",
          "name": "write person",
          "environments": [
            "production",
            "test"
          ],
          "output": "person",
          "inputView": "${inputView}",
          "saveMode": "Overwrite",
          "params": {
            "es.nodes": "${esURL}",
            "es.port": "${port}",
            "es.nodes.wan.only": "${wanOnly}",
            "es.net.ssl": "${ssl}"
          }
        }
      ]
    }"""
    
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(_) => {
        println(pipelineEither)
        assert(false)
      }
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)
    }  
  } 
}
