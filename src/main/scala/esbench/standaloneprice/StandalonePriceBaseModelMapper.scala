package esbench.standaloneprice

import esbench.DomainModelMapper
import cats.implicits._

import esbench.standaloneprice.{Product, ProductQuery, ProductStats}
trait StandalonePriceBaseModelMapper extends DomainModelMapper[Product, ProductQuery, ProductStats]:
  def statsEmpty: ProductStats = ProductStats.empty
  def stats(ds: Seq[Product]): ProductStats = ProductStats.from(ds)
  def combineStats(a: ProductStats, b: ProductStats): ProductStats =
    a.combine(b)