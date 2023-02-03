package esbench

import cats.effect.IOApp
import cats.effect.IO
import sttp.client3.httpclient.cats.HttpClientCatsBackend
import sttp.model.Uri

import cats.effect.std.Random

import cats.implicits._
import esbench.standaloneprice.StandalonePriceDomainRunConf

import esbench.DefaultElasticsearchClient
object MongoMain extends IOApp.Simple {

  val esHostIO = IO.fromEither(
    Uri
      .parse("http://localhost:10200")
      .leftMap(err => new IllegalArgumentException(err))
  )

  val staticRunConfs = List(
    StaticRunConfiguration(
      esShards = 3,
      esShardsReplicas = 0,
      domainModelNr = 10,
      queryNr = 1000
    ),
    StaticRunConfiguration(
      esShards = 3,
      esShardsReplicas = 0,
      domainModelNr = 100,
      queryNr = 1000
    ),
    StaticRunConfiguration(
      esShards = 3,
      esShardsReplicas = 0,
      domainModelNr = 1000,
      queryNr = 1000
    ),
    StaticRunConfiguration(
      esShards = 3,
      esShardsReplicas = 0,
      domainModelNr = 10000,
      queryNr = 1000
    ),
    StaticRunConfiguration(
      esShards = 3,
      esShardsReplicas = 0,
      domainModelNr = 100000,
      queryNr = 1000
    )
  )

  def runConfs(using Random[IO]) = staticRunConfs.flatMap(c =>
    List(
      RunConfiguration(
        c,
        StandalonePriceDomainRunConf.nested
      ),
      RunConfiguration(
        c,
        StandalonePriceDomainRunConf.pricePropagated
      ),
      RunConfiguration(
        c,
        StandalonePriceDomainRunConf.parentChild3Level
      ),
      RunConfiguration(
        c,
        StandalonePriceDomainRunConf.joinedIndexes
      )
    )
  )

  val parallelism = 10
  val maxBatchSize = 100

  val run = for {
    esUri <- esHostIO
    given Random[IO] <- Random.scalaUtilRandom[IO]
    sttp <- HttpClientCatsBackend.resource[IO]().use { implicit sttp =>
      val esClient = new DefaultElasticsearchClient(esUri)
      val runner = new RunExecutor(esClient, parallelism, maxBatchSize)
      val runs = for {
        conf <- runConfs
      } yield runner.run(conf)

      runs.sequence
    }
    _ <- IO.println("== Done")
  } yield ()
}

class MongoClient extends SearchBenchClient


