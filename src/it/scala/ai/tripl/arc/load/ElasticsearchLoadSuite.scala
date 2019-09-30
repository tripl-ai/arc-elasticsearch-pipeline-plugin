package ai.tripl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties
import scala.util.Random
import org.apache.http.client.methods.{HttpDelete, HttpGet}
import org.apache.http.impl.client.HttpClientBuilder
import com.fasterxml.jackson.databind.ObjectMapper

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._
import ai.tripl.arc.config.ArcPipeline
import org.elasticsearch.spark.sql._

class ElasticsearchLoadSuite extends FunSuite with BeforeAndAfter {


  val alpha = "abcdefghijklmnopqrstuvwxyz"
  val size = alpha.size
  def randStr(n:Int) = (1 to n).map(x => alpha(Random.nextInt.abs % size)).mkString

  var session: SparkSession = _
  val testData = getClass.getResource("/akc_breed_info.csv").toString
  val inputView = "expected"
  val index = "dogs"
  val esURL = "elasticsearch"
  val port = "9200"
  val wanOnly = "true"
  val ssl = "false"
  val streamingIndex = "streaming"
  val outputView = "outputView"
  val checkpointLocation = "/tmp/checkpointLocation"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("es.index.auto.create", "true")
                  .config("spark.sql.streaming.checkpointLocation", checkpointLocation)
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
    val delete = new HttpDelete(s"http://${esURL}:9200/${index}")
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
        outputMode=OutputModeTypeAppend,
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

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(_) => {
        println(pipelineEither)
        assert(false)
      }
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)
    }
  }

  test("ElasticsearchLoad: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)
    FileUtils.deleteQuietly(new java.io.File(checkpointLocation))

    val indexName = randStr(10)

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView(inputView)

    load.ElasticsearchLoadStage.execute(
      load.ElasticsearchLoadStage(
        plugin=new load.ElasticsearchLoad,
        name="df",
        description=None,
        inputView=inputView,
        output=indexName,
        numPartitions=None,
        params=Map("es.nodes.wan.only" -> wanOnly, "es.port" -> port, "es.net.ssl" -> ssl, "es.nodes" -> esURL),
        saveMode=SaveMode.Overwrite,
        outputMode=OutputModeTypeAppend,
        partitionBy=Nil
      )
    )
    Thread.sleep(2000)

    spark.streams.active.foreach(streamingQuery => streamingQuery.stop)

    // call _search rest api to get all documents for new index
    val client = HttpClientBuilder.create.build
    val get = new HttpGet(s"http://${esURL}:${port}/${indexName}/_search")
    val response = client.execute(get)
    val body = Source.fromInputStream(response.getEntity.getContent).mkString
    response.close

    // assert that the documents array returned in the search is not empty
    // if no documents then hits.hits will fail anyway
    spark.read.json(spark.sparkContext.parallelize(Seq(body)).toDF.as[String]).createOrReplaceTempView("response")
    val hitsSize = spark.sql("""
    SELECT SIZE(hits.hits) FROM response
    """)
    assert(hitsSize.first.getInt(0) != 0)
  }
}