package esbench

import cats.effect.IO
import sttp.client3.SttpBackend

import cats.implicits._
import scala.concurrent.duration.FiniteDuration
import cats.kernel.Monoid
import scala.concurrent.duration.Duration

case class StaticRunConfiguration(
    esShards: Int,
    esShardsReplicas: Int,
    domainModelNr: Int,
    queryNr: Int
):
  def withDomainRunConfiguration[D, Q, S](
      conf: DomainRunConfiguration[D, Q, S]
  ): RunConfiguration[D, Q, S] =
    RunConfiguration(this, conf)

case class DomainRunConfiguration[D, Q, S](
    domainA: IO[D],
    queryA: IO[Q],
    mapper: DomainModelMapper[D, Q, S]
)

case class RunConfiguration[D, Q, S](
    static: StaticRunConfiguration,
    domain: DomainRunConfiguration[D, Q, S]
)

case class Stats[T](
    min: T,
    max: T,
    avg: T,
    p50: T,
    p75: T,
    p90: T,
    p95: T,
    p99: T
):
  override def toString(): String =
    s"Stats(min=${min}, max=${max}, avg=${avg}, p50=${p50}, p75=${p75}, p90=${p90}, p95=${p95}, p99=${p99})"

object Stats:
  private def percentile[T](p: Int, sorted: Seq[T]): T =
    val idx = (p.toDouble / 100 * sorted.size).round.toInt
    sorted(math.min(math.max(0, idx), sorted.size - 1))

  def fromInts(entries: Seq[Int]): Stats[Int] =
    val sorted = entries.sorted
    Stats(
      sorted.min,
      sorted.max,
      sorted.sum / sorted.size,
      percentile(50, sorted),
      percentile(75, sorted),
      percentile(90, sorted),
      percentile(95, sorted),
      percentile(99, sorted)
    )

class RunExecutor(
    esClient: ElasticsearchClient,
    parallelism: Int,
    maxBulkSize: Int
):
  private def indexStage[D, S](
      domainModelNr: Int,
      domainModelA: IO[D],
      mapper: DomainModelMapper[D, _, S]
  ): IO[(FiniteDuration, Map[String, Int], S)] =
    def go(
        n: Int,
        done: Int,
        stats: S,
        totalTime: FiniteDuration,
        totalExpected: Map[String, Int]
    ): IO[(FiniteDuration, Map[String, Int], S)] =
      for
        _ <- IO.print(
          s"\r* processing domain model batch ${done} to ${done + n}"
        )
        domainModels <- IO.parReplicateAN(parallelism)(n, domainModelA)
        docs <- IO(domainModels.flatMap(mapper.toElasticModel))

        indexTime <- esClient.indexDocumentsBulk(docs).timed.map(_._1)
        expected = docs.groupBy(_.index).mapValues(_.size).toMap

        updatedStats = mapper.combineStats(stats, mapper.stats(domainModels))

        remaining = math.max(
          0,
          math.min(maxBulkSize, domainModelNr - (done + n))
        )
        updatedTime = totalTime + indexTime
        updatedExpected = totalExpected.combine(expected)

        res <-
          if (remaining > 0)
            go(
              remaining,
              done + n,
              updatedStats,
              updatedTime,
              updatedExpected
            )
          else
            IO.println("\nIndexing done") *>
              IO.pure(
                (
                  updatedTime,
                  updatedExpected,
                  updatedStats
                )
              )
      yield res
    end go

    go(
      math.min(maxBulkSize, domainModelNr),
      0,
      mapper.statsEmpty,
      Duration.Zero,
      Map.empty
    )

  private def searchStage[D, Q](
      domainQueryA: IO[Q],
      queryNr: Int,
      mapper: DomainModelMapper[D, Q, _]
  ): IO[(Stats[Int], Stats[Int])] =
    import DomainModelMapping._

    val queryA = for
      domainQuery <- domainQueryA
      res <- mapper.query(domainQuery) match {
        case SingleElasticQuery(index, query) =>
          esClient.search(index, query).map(r => r.took -> r.hits.total.value)
        case SequencedElasticQuery(head, tail) =>
          val hRespA = esClient.search(head.index, head.query)

          val queryA = tail.foldLeft(hRespA.map(0 -> _)) { case (ioa, queryF) =>
            for
              respWithTimeAcc <- ioa
              (tookAcc, resp) = respWithTimeAcc
              nextQuery = queryF(resp.hits)
              nextResp <- esClient
                .search(nextQuery.index, nextQuery.query)
                .map { resp => (tookAcc + resp.took) -> resp }
            yield nextResp
          }

          queryA.map { case (tookAcc, r) => tookAcc -> r.hits.total.value }
      }
    yield res

    IO.parReplicateAN(parallelism)(queryNr, queryA).map { is =>
      val (ts, hs) = is.unzip
      Stats.fromInts(ts) -> Stats.fromInts(hs)
    }

  def run[D, Q, S](conf: RunConfiguration[D, Q, S]) =
    for {
      _ <- IO.println(
        s"=== Starting run: domain-models=${conf.static.domainModelNr} queries=${conf.static.queryNr} domainMapper=${conf.domain.mapper.getClass().getSimpleName()}"
      )
      indexMappings = conf.domain.mapper.esMappings
      _ <- indexMappings.toList.map { case (index, mapping) =>
        IO.println(s"* initializing index ${index}") *>
          esClient.deleteIndex(index) *>
          esClient.createIndex(
            index,
            conf.static.esShards,
            conf.static.esShardsReplicas
          ) *>
          esClient.createMappings(index, mapping)
      }.sequence

      _ <- IO.println("* indexing domain models")
      indexRes <- indexStage[D, S](
        conf.static.domainModelNr,
        conf.domain.domainA,
        conf.domain.mapper
      )
      (indexTime, expected, genStats) = indexRes

      _ <- IO.println(
        s"Indexing statistics:\n\t- Domain models: ${genStats}\n\t- Time for indexing: ${indexTime.toMillis} milliseconds"
      )

      totalDocs = expected.values.sum
      _ <- IO.println(
        s"* waiting for ES to completely index ${totalDocs} documents"
      )
      _ <- esClient.waitForIndexCounts(expected)

      _ <- IO.println("* searching")
      searchRes <- searchStage[D, Q](
        conf.domain.queryA,
        conf.static.queryNr,
        conf.domain.mapper
      )
      (searchTimeMs, searchHits) = searchRes

      _ <- IO.println(
        s"Query statistics:\n\t- time: ${searchTimeMs}\n\t- hits: ${searchHits}"
      )
    } yield ()
