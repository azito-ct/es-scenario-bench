import cats.effect.IOApp
import cats.effect.IO
import sttp.client3.httpclient.cats.HttpClientCatsBackend
import sttp.client3.quick._
import sttp.model.Uri

import cats.implicits._
import sttp.client3.SttpBackend
import sttp.client3.circe._
import sttp.model.StatusCode

import io.circe.generic.auto._
import io.circe.syntax._
import cats.effect.std.Random
import cats.Parallel

import scala.concurrent.duration._
import io.circe.Json
import sttp.client3.Response
import sttp.client3.Request
import io.circe.Decoder
import cats.data.Validated

trait Scenario {

  def indexName: String

  def documentsNr: Int
  def queryNr: Int

  def mapping: Json

  def generateModel(r: Random[IO]): IO[(String, Json)]

  def query: Json
}

object BaseScenario extends Scenario:

  override def documentsNr: Int = 1000

  override def queryNr: Int = 100

  case class BaseProduct(id: String, centAmount: Int, productType: String)

  override def indexName: String = "basic"

  override def mapping: Json = Json.obj(
    "dynamic" := "false",
    "properties" := Json.obj(
      "id" := Json.obj("type" := "keyword"),
      "productType" := Json.obj("type" := "keyword"),
      "centAmount" := Json.obj("type" := "long")
    )
  )

  override def generateModel(r: Random[IO]): IO[(String, Json)] = for {
    id <- IO.randomUUID
    centAmount <- r.nextIntBounded(1000)
    ptype <- r.oneOf("p1", "p2", "p3")
  } yield id.toString() -> BaseProduct(id.toString(), centAmount, ptype).asJson

  override def query: Json = Json.obj(
    "query" := Json.obj(
      "bool" := Json.obj(
        "filter" := List(
          Json.obj("term" := Json.obj("productType" := "p1")),
          Json.obj(
            "range" := Json.obj(
              "centAmount" := Json.obj("gte" := 250, "lte" := 750)
            )
          )
        )
      )
    ),
    "sort" := List(
      Json.obj("centAmount" := Json.obj("order" := "ASC"))
    ),
    "aggs" := Json.obj(
      "totalPrice" := Json.obj(
        "sum" := Json.obj("field" := "centAmount")
      )
    )
  )

class ScenarioRunner(esHost: Uri, parallelism: Int) {
  private def validateRespDefault[Resp]
      : PartialFunction[(StatusCode, Either[String, Resp]), Resp] = {
    case (code, Right(r: Resp)) if code.isSuccess => r
  }

  private def simpleRequest[Resp](
      descr: String,
      req: Request[_, Any],
      validateF: PartialFunction[(StatusCode, Either[String, Resp]), Resp] =
        validateRespDefault[Resp]
  )(implicit backend: SttpBackend[IO, Any], decoder: Decoder[Resp]): IO[Resp] =
    for {
      resp <- backend.send(req.response(asJson[Resp])).recoverWith { case err =>
        IO.raiseError(
          new IllegalArgumentException(s"Request error ${descr}", err)
        )
      }
      respBody = resp.body.leftMap(_.getMessage())
      valid = validateF.lift(resp.code, respBody)
      result <- IO.fromOption(valid)(
        new IllegalArgumentException(
          s"Response error ${descr}: code=${resp.code} resp=${respBody}"
        )
      )
    } yield result

  case class DeleteIndexResponse(acknowledged: Boolean)
  def deleteIndex(scenario: Scenario)(implicit
      backend: SttpBackend[IO, Any]
  ): IO[Unit] =
    val req = quickRequest.delete(esHost.withPath(scenario.indexName))
    simpleRequest[DeleteIndexResponse](
      "delete index",
      req,
      {
        case (_, Right(r @ DeleteIndexResponse(true))) => r
        case (StatusCode.NotFound, _) => DeleteIndexResponse(false)
      }
    ).void

  case class CreateIndexResponse(acknowledged: Boolean, index: String)
  def createIndex(scenario: Scenario)(implicit
      backend: SttpBackend[IO, Any]
  ): IO[Unit] =
    val req = quickRequest.put(esHost.withPath(scenario.indexName))
    simpleRequest[CreateIndexResponse]("create index", req).void

  case class CreateMappingResponse(acknowledged: Boolean)
  def createMapping(scenario: Scenario)(implicit
      backend: SttpBackend[IO, Any]
  ): IO[Unit] =
    val req = quickRequest.put(esHost.withPath(scenario.indexName, "_mappings")).body(scenario.mapping)
    simpleRequest[CreateMappingResponse]("create mapping", req).void

  case class IndexDocumentResponse(_id: String, _index: String)
  def indexDocuments(scenario: Scenario)(implicit
      backend: SttpBackend[IO, Any]
  ): IO[Unit] =
    for {
      random <- Random.scalaUtilRandom[IO]
      indexSingleDoc = for {
        docWithId <- scenario.generateModel(random)
        (id, doc) = docWithId
        req = quickRequest
          .put(esHost.withPath(scenario.indexName, "_doc", id))
          .body(doc)
        _ <- simpleRequest[IndexDocumentResponse]("index document", req)
      } yield ()
      _ <- IO.parReplicateAN(parallelism)(scenario.documentsNr, indexSingleDoc)
    } yield ()

  case class CountResponse(count: Int)
  def waitForIndexingCompletion(
      scenario: Scenario
  )(implicit backend: SttpBackend[IO, Any]): IO[Unit] =
    for {
      count <- simpleRequest[CountResponse](
        "waiting for indexing",
        quickRequest.get(esHost.withPath(scenario.indexName, "_count"))
      )
      _ <-
        if (count.count >= scenario.documentsNr) IO.unit
        else IO.delay(100.milli) *> waitForIndexingCompletion(scenario)
    } yield ()

  case class SearchHitsTotal(value: Int)
  case class SearchHit(_id: String, _score: Double, _source: Json)
  case class SearchHits(total: SearchHitsTotal, hits: List[SearchHit])
  case class SearchResponse(took: Int, hits: SearchHits, aggregations: Json)
  def search(scenario: Scenario)(implicit
      backend: SttpBackend[IO, Any]
  ): IO[FiniteDuration] =
    val req = quickRequest
      .get(esHost.withPath(scenario.indexName, "_search"))
      .body(scenario.query)
    val queryES = simpleRequest[SearchResponse]("searching", req).map(_.took)
    IO.parReplicateAN(parallelism)(scenario.queryNr, queryES).map(_.sum.millis)
}

object Main extends IOApp.Simple {

  val esHostIO = IO.fromEither(
    Uri
      .parse("http://localhost:10200")
      .leftMap(err => new IllegalArgumentException(err))
  )

  val scenario = BaseScenario
  val parallelism = 10

  val run = for {
    esHost <- esHostIO
    runner = new ScenarioRunner(esHost, parallelism)
    sttp <- HttpClientCatsBackend.resource[IO]().use { implicit backend =>
      for {
        _ <- runner.deleteIndex(scenario)
        _ <- runner.createIndex(scenario)
        _ <- runner.createMapping(scenario)
        indexTime <- runner.indexDocuments(scenario).timed.map(_._1)
        _ <- IO.println(s"Indexing took ${indexTime.toMillis} milliseconds")

        _ <- runner.waitForIndexingCompletion(scenario)

        queryTime <- runner.search(scenario)
        _ <- IO.println(s"Query took ${queryTime.toMillis} milliseconds")
      } yield ()
    }
  } yield ()
}
