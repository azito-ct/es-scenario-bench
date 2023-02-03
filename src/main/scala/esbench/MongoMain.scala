package esbench

import cats.effect.IOApp
import cats.effect.IO
import sttp.client3.httpclient.cats.HttpClientCatsBackend
import sttp.model.Uri

import cats.effect.std.Random

import cats.implicits._
import esbench.standaloneprice.StandalonePriceDomainRunConf

import esbench.DefaultElasticsearchClient
import io.circe.Json
import esbench.ElasticsearchModel._

import org.mongodb.scala._
import esbench.standaloneprice.MongoJoinedIndexesModelMapper
import org.bson.conversions.Bson
import org.mongodb.scala.bson.BsonDocument

object MongoMain extends IOApp.Simple {
  val staticRunConfs = List(
    StaticRunConfiguration(
      esShards = 3,
      esShardsReplicas = 0,
      domainModelNr = 10000,
      queryNr = 1
    )
    // StaticRunConfiguration(
    //   esShards = 3,
    //   esShardsReplicas = 0,
    //   domainModelNr = 100,
    //   queryNr = 1000
    // ),
    // StaticRunConfiguration(
    //   esShards = 3,
    //   esShardsReplicas = 0,
    //   domainModelNr = 1000,
    //   queryNr = 1000
    // ),
    // StaticRunConfiguration(
    //   esShards = 3,
    //   esShardsReplicas = 0,
    //   domainModelNr = 10000,
    //   queryNr = 1000
    // ),
    // StaticRunConfiguration(
    //   esShards = 3,
    //   esShardsReplicas = 0,
    //   domainModelNr = 100000,
    //   queryNr = 1000
    // )
  )

  def runConfs(using Random[IO]) = staticRunConfs.flatMap(c =>
    List(
      RunConfiguration(
        c,
        StandalonePriceDomainRunConf.mongoJoinedIndexes
      )
    )
  )

  val parallelism = 10
  val maxBatchSize = 100

  val run = for {
    given Random[IO] <- Random.scalaUtilRandom[IO]
    given MongoClient <- IO(MongoClient("mongodb://localhost:28017"))
    _ <- {
      val esClient = new MongoBenchClient("test")
      val runner = new RunExecutor(esClient, parallelism, maxBatchSize)
      val runs = for {
        conf <- runConfs
      } yield runner.run(conf)

      runs.sequence
    }
    _ <- IO.println("== Done")
  } yield ()
}

class MongoBenchClient(dbName: String)(using client: MongoClient)
    extends SearchBenchClient:
  import io.circe.syntax._
  import io.circe.generic.auto._
  import cats.implicits._

  private def toDocument(
      esDoc: ElasticDoc
  ): org.mongodb.scala.bson.collection.immutable.Document =
    Document(esDoc.doc.deepMerge(Json.obj("_id" := esDoc.id)).noSpaces)

  override def createIndex(
      indexName: String,
      shards: Int,
      replicas: Int
  ): IO[Unit] =
    IO(client.getDatabase(dbName).createCollection(indexName)).void

  def deleteIndex(indexName: String): IO[Unit] =
    IO.fromFuture(
      IO(client.getDatabase(dbName).getCollection(indexName).drop().toFuture())
    ).void

  def createMappings(indexName: String, mapping: Json): IO[Unit] =
    for {
      mongoDb <- IO(client.getDatabase(dbName))
      indexes <- IO.fromEither(mapping.as[List[Json]])
      _ <- indexes.iterator
        .map { indexDef =>
          IO.fromFuture(
            IO(
              mongoDb
                .getCollection(indexName)
                .createIndex(BsonDocument(indexDef.noSpaces))
                .toFuture()
            )
          )

        }
        .toList
        .sequence
    } yield ()

  def indexDocumentsBulk(
      docs: List[ElasticDoc],
      maxBatchSize: Int = 1000
  ): IO[Unit] =
    val docsByCollection =
      docs.groupBy(_.index).view.mapValues(_.map(toDocument)).toMap

    for {
      mongoDb <- IO(client.getDatabase(dbName))
      res <- docsByCollection
        .map { case (coll, docs) =>
          IO.fromFuture(
            IO(mongoDb.getCollection(coll).insertMany(docs).toFuture())
          )
        }
        .toList
        .sequence
    } yield ()

  def waitForIndexCounts(expected: Map[String, Int]): IO[Unit] = IO.unit
  def search(
      indexName: String,
      query: Json
  ): IO[SearchResponse] =
    for {
      pipelineStages <- IO.pure(
        query.asArray.map(_.map(j => BsonDocument(j.noSpaces))).getOrElse(Nil)
      )
      //_ <- IO.println(pipelineStages)
      res <- IO.fromFuture(
        IO(
          client
            .getDatabase(dbName)
            .getCollection(indexName)
            .aggregate(pipelineStages)
            .toFuture()
        )
      ).timed

      (took, docs) = res
    } yield SearchResponse(took.toMillis.toInt, SearchHits(SearchHitsTotal(docs.size), Nil), None)
