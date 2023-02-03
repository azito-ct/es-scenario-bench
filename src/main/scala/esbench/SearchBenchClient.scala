package esbench

import io.circe.syntax._
import io.circe.generic.auto._
import io.circe._
import io.circe.Json
import io.circe.JsonObject

import sttp.client3._
import sttp.client3.circe._
import sttp.model._

import cats.implicits._
import cats.effect.IO

import scala.concurrent.duration._
import esbench.ElasticsearchModel.ElasticDoc
import esbench.ElasticsearchModel.SearchResponse

object ElasticsearchModel:
  case class DeleteIndexResponse(acknowledged: Boolean)
  case class CreateIndexResponse(acknowledged: Boolean, index: String)
  case class CreateMappingResponse(acknowledged: Boolean)
  case class IndexDocumentResponse(_id: String, _index: String)
  case class BulkResponse(errors: Boolean)
  case class CountResponse(count: Int)

  case class SearchHitsTotal(value: Int)
  case class SearchHit(
      _id: String,
      _score: Double,
      _source: Option[Json],
      fields: Option[Json],
      inner_hits: Option[Json]
  )
  case class SearchHits(total: SearchHitsTotal, hits: List[SearchHit])
  case class SearchResponse(
      took: Int,
      hits: SearchHits,
      aggregations: Option[Json]
  )

  case class ElasticDoc(
      index: String,
      id: String,
      doc: Json,
      routing: Option[String] = None
  )
end ElasticsearchModel

trait SearchBenchClient:
  def createIndex(indexName: String, shards: Int, replicas: Int): IO[Unit]
  def deleteIndex(indexName: String): IO[Unit]
  def createMappings(indexName: String, mapping: Json): IO[Unit]
  def indexDocumentsBulk(
      docs: List[ElasticDoc],
      maxBatchSize: Int = 1000
  ): IO[Unit]
  def waitForIndexCounts(expected: Map[String, Int]): IO[Unit]
  def search(
      indexName: String,
      query: Json
  ): IO[SearchResponse]

class DefaultElasticsearchClient(esUri: Uri)(using sttp: SttpBackend[IO, Any]) extends SearchBenchClient:
  import ElasticsearchModel._
  private def retryWithBackoff[A](
      ioa: IO[A],
      initialDelay: FiniteDuration,
      maxRetries: Int
  ): IO[A] =
    ioa.handleErrorWith { error =>
      if (maxRetries > 0)
        IO.sleep(initialDelay) *> retryWithBackoff(
          ioa,
          initialDelay * 2,
          maxRetries - 1
        )
      else
        IO.raiseError(error)
    }

  private def validateRespDefault[Resp]
      : PartialFunction[(StatusCode, Either[String, Resp]), Resp] =
    case (code, Right(r: Resp)) if code.isSuccess => r

  private def simpleRequest[Resp](
      descr: String,
      req: Request[_, Any],
      validateF: PartialFunction[(StatusCode, Either[String, Resp]), Resp] =
        validateRespDefault[Resp]
  )(using Decoder[Resp]): IO[Resp] =
    for {
      resp <- retryWithBackoff(
        sttp.send(req.response(asJson[Resp])).recoverWith { case err =>
          IO.raiseError(
            new IllegalArgumentException(s"Request error [${descr}]", err)
          )
        },
        50.millis,
        3
      )
      respBody = resp.body.leftMap(_.getMessage())
      valid = validateF.lift(resp.code, respBody)
      result <- IO.fromOption(valid)(
        new IllegalArgumentException(
          s"Response error [${descr}]: code=${resp.code} resp=${respBody}"
        )
      )
    } yield result

  override def createIndex(indexName: String, shards: Int, replicas: Int): IO[Unit] =
    val req = quickRequest
      .put(esUri.withPath(indexName))
      .body(
        Json.obj(
          "settings" := Json.obj(
            "number_of_shards" := shards,
            "number_of_replicas" := replicas
          )
        )
      )
    simpleRequest[CreateIndexResponse]("create index", req).void

  override def deleteIndex(indexName: String): IO[Unit] =
    val req = quickRequest.delete(esUri.withPath(indexName))
    simpleRequest[DeleteIndexResponse](
      "delete index",
      req,
      {
        case (_, Right(r @ DeleteIndexResponse(true))) => r
        case (StatusCode.NotFound, _) => DeleteIndexResponse(false)
      }
    ).void

  override def createMappings(indexName: String, mapping: Json): IO[Unit] =
    val req = quickRequest
      .put(esUri.withPath(indexName, "_mappings"))
      .body(mapping)
    simpleRequest[CreateMappingResponse]("create mapping", req).void

  def indexDocument(doc: ElasticDoc)(implicit
      backend: SttpBackend[IO, Any]
  ): IO[Unit] =
    val reqUrlBase = esUri.withPath(doc.index, "_doc", doc.id)
    val reqUrl = doc.routing.fold(reqUrlBase)(routingId =>
      reqUrlBase.withParam("routing", routingId)
    )
    val req =
      quickRequest
        .put(reqUrl)
        .body(doc.doc)

    simpleRequest[IndexDocumentResponse]("index document", req).void

  override def indexDocumentsBulk(
      docs: List[ElasticDoc],
      maxBatchSize: Int = 1000
  ): IO[Unit] =
    def bulkReq(
        index: String,
        routing: Option[String],
        docs: Seq[ElasticDoc]
    ): IO[BulkResponse] =
      val baseReqUrl = esUri.withPath(index, "_bulk")
      val reqUrl =
        routing.fold(baseReqUrl)(r => baseReqUrl.withParam("routing", r))
      val actions = docs.iterator
        .map { d =>
          val action = Json
            .obj("index" := Json.obj("_index" := d.index, "_id" := d.id))
            .noSpaces

          s"$action\n${d.doc.noSpaces}"
        }
        .mkString("\n")
      val req =
        quickRequest
          .put(reqUrl)
          .body(s"${actions}\n")
          .contentType("application/json")

      simpleRequest[BulkResponse](
        "index document",
        req,
        { case (StatusCode.Ok, Right(r @ BulkResponse(false))) => r }
      )

    val docsByIndexRouting = docs.groupBy(d => d.index -> d.routing)
    docsByIndexRouting.iterator
      .map { case ((index, routing), docs) =>
        docs
          .grouped(maxBatchSize)
          .map(bulkReq(index, routing, _))
          .toList
          .sequence_
      }
      .toList
      .sequence_

  override def waitForIndexCounts(expected: Map[String, Int]): IO[Unit] =
    def go(indexName: String, expectedSize: Int): IO[Unit] =
      for {
        count <- simpleRequest[CountResponse](
          "waiting for indexing",
          quickRequest.get(esUri.withPath(indexName, "_count"))
        )
        _ <-
          if (count.count >= expectedSize) IO.unit
          else IO.delay(100.milli) *> go(indexName, expectedSize)
      } yield ()

    expected.toList
      .map { case (indexName, expectedSize) => go(indexName, expectedSize) }
      .sequence
      .void

  override def search(
      indexName: String,
      query: Json
  ): IO[SearchResponse] =
    val reqUrl =
      quickRequest
        .get(esUri.withPath(indexName, "_search"))
        .body(query)

    val req = reqUrl.body(query)

    simpleRequest[SearchResponse](
      "searching",
      req
    )
