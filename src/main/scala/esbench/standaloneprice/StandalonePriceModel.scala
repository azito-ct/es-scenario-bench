package esbench.standaloneprice

import cats.effect.std.Random
import cats.effect.IO

import cats.implicits._
import cats.kernel.Monoid

object StandalonePriceModel:
  val currencies = Vector(
    "USD",
    "GBP",
    "EUR",
    "CHF",
    "NOK"
  )

  val currenciesSet = currencies.toSet

  val countries = Vector(
    "us",
    "uk",
    "de",
    "fr",
    "it",
    "es",
    "pt",
    "at",
    "nw"
  ).map(Some(_)) :+ None

  val countriesSet = countries.toSet

  val maxProductTypes = 10
  def productType(i: Int): String = s"productType-$i"
  val productTypes = 0.to(maxProductTypes).map(i => productType(i)).toSet

  val maxVariants = 100

  val maxVariantsFilterField = 100

  val maxChannels = 100 / (currencies.size + countries.size)
  def channel(i: Int): String = s"channel-$i"

  def randomSubset[T](set: Set[T])(using r: Random[IO]): IO[Set[T]] =
    def go(subset: Vector[T], n: Int, acc: Set[T]): IO[Set[T]] =
      if (n > 0)
        for
          idx <- r.nextIntBounded(subset.size)
          v = subset(idx)
          rest = subset.take(idx) ++ subset.drop(idx + 1)
          res <- go(rest, n - 1, acc + v)
        yield res
      else IO.pure(acc)

    for
      n <- r.nextIntBounded(set.size)
      res <- go(set.toVector, n, Set())
    yield res

case class PriceDescriptor(
    id: String,
    centAmount: Int,
    discountedCentAmount: Option[Int]
)

object PriceDescriptor:
  def random(using r: Random[IO]): IO[PriceDescriptor] =
    for
      id <- IO.randomUUID
      price <- r.nextIntBounded(100_000)
      discounted <- r.nextIntBounded(99_000).map {
        case 0 => None
        case i => Some(i)
      }
    yield PriceDescriptor(id.toString, price, discounted)

case class PriceSelector(
    currency: String,
    country: Option[String],
    channel: Option[String]
)

object PriceSelector:
  def random(using r: Random[IO]): IO[Set[PriceSelector]] =
    for
      currN <- r.betweenInt(1, StandalonePriceModel.currencies.size)
      currs <- StandalonePriceModel.randomSubset(StandalonePriceModel.currenciesSet)
      countryN <- r.nextIntBounded(StandalonePriceModel.countries.size)
      countries <- StandalonePriceModel.randomSubset(StandalonePriceModel.countriesSet)
      channelNr <- r.nextIntBounded(StandalonePriceModel.maxChannels)
      channels = 0
        .to(channelNr)
        .map {
          case 0 => None
          case i => Some(StandalonePriceModel.channel(i))
        }
        .toSet
      res = for
        curr <- currs
        country <- countries
        channel <- channels
      yield PriceSelector(curr, country, channel)
    yield res

case class ProductVariant(
    sku: String,
    name: String,
    filterField: Int,
    prices: Map[PriceSelector, PriceDescriptor]
)

object ProductVariant:
  def random(using r: Random[IO]): IO[ProductVariant] =
    for
      sku <- r.nextPrintableChar.replicateA(16)
      name <- r.nextPrintableChar.replicateA(10)
      filterField <- r.nextIntBounded(StandalonePriceModel.maxVariantsFilterField)
      priceSelectors <- PriceSelector.random
      prices <- PriceDescriptor.random.replicateA(priceSelectors.size)
      priceMap = priceSelectors.zip(prices).toMap
    yield ProductVariant(sku.mkString, name.mkString, filterField, priceMap)

case class Product(
    id: String,
    productType: String,
    variants: List[ProductVariant]
)

object Product:
  def random(using r: Random[IO]): IO[Product] =
    for
      id <- IO.randomUUID
      ptypeNr <- r.nextIntBounded(StandalonePriceModel.maxProductTypes)
      ptype = StandalonePriceModel.productType(ptypeNr)
      variantsNr <- r.nextIntBounded(StandalonePriceModel.maxVariants)
      variants <- ProductVariant.random.replicateA(variantsNr)
    yield Product(id.toString(), ptype, variants)

case class ProductQuery(
    productType: Set[String],
    priceSelector: PriceSelector,
    variantFilterField: Int,
    centAmountRange: Range
)

object ProductQuery:
  def simple(using r: Random[IO]): IO[ProductQuery] =
    for
      types <- StandalonePriceModel.randomSubset(StandalonePriceModel.productTypes)
      currencyIdx <- r.nextIntBounded(StandalonePriceModel.currencies.size)
      currency = StandalonePriceModel.currencies(currencyIdx)
      countryIdx <- r.nextIntBounded(StandalonePriceModel.countries.size)
      country = StandalonePriceModel.countries(countryIdx)
      channelIdx <- r.nextIntBounded(StandalonePriceModel.maxChannels)
      channel_ = Option.when(channelIdx > 0)(StandalonePriceModel.channel(channelIdx))
      variantFilterField <- r.nextIntBounded(StandalonePriceModel.maxVariantsFilterField)
    yield ProductQuery(
      productType = types,
      variantFilterField = variantFilterField,
      priceSelector = PriceSelector(currency, country, channel_),
      centAmountRange = Range(0, 100_000)
    )

case class ProductStats(products: Int, variants: Int, prices: Int)
object ProductStats:
  val empty = ProductStats(0, 0, 0)
  def from(p: Product): ProductStats =
    ProductStats(1, p.variants.size, p.variants.map(_.prices.size).sum)

  def from(ps: Seq[Product]): ProductStats =
    ps.foldMap(from)

  given Monoid[ProductStats] = Monoid.instance(
    empty,
    (a, b) =>
      ProductStats(
        a.products + b.products,
        a.variants + b.variants,
        a.prices + b.prices
      )
  )
