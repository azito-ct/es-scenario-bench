package esbench.standaloneprice

import esbench.DomainRunConfiguration
import cats.effect.std.Random
import cats.effect.IO

import esbench.standaloneprice.NestedModelMapper
object StandalonePriceDomainRunConf:
  def nested(using Random[IO]) = DomainRunConfiguration(
    domainA = Product.random,
    queryA = ProductQuery.simple,
    mapper = NestedModelMapper
  )

  def pricePropagated(using Random[IO]) = DomainRunConfiguration(
    domainA = Product.random,
    queryA = ProductQuery.simple,
    mapper = PricePropagatedModelMapper
  )

  def parentChild3Level(using Random[IO]) = DomainRunConfiguration(
    domainA = Product.random,
    queryA = ProductQuery.simple,
    mapper = ParentChild3LevelModelMapper
  )

  def joinedIndexes(using Random[IO]) = DomainRunConfiguration(
    domainA = Product.random,
    queryA = ProductQuery.simple,
    mapper = JoinedIndexesModelMapper
  )