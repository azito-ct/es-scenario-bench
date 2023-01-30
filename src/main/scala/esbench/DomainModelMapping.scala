package esbench

import io.circe.Json

object DomainModelMapping:
  sealed trait ElasticQuery
  case class SingleElasticQuery(index: String, query: Json) extends ElasticQuery
  case class SequencedElasticQuery(head: SingleElasticQuery, tail: List[ElasticsearchModel.SearchHits => SingleElasticQuery]) extends ElasticQuery


trait DomainModelMapper[D, Q, S]:
  def esMappings: Map[String, Json]

  def toElasticModel(model: D): Seq[ElasticsearchModel.ElasticDoc]

  def query(query: Q): DomainModelMapping.ElasticQuery

  def statsEmpty: S
  def stats(ds: Seq[D]): S
  def combineStats(a: S, b: S): S