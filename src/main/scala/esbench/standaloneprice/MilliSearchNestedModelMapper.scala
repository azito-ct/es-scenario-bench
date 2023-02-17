package esbench.standaloneprice

import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import esbench.ElasticsearchModel
import esbench.DomainModelMapping

object MilliSearchNestedModelMapper extends StandalonePriceBaseModelMapper:
  override def esMappings: Map[String, Json] = Map(
    "products" -> Json.obj(
      "uid" := "products",
      "primaryKey" := "id",
      "settings" := Json.obj(
        "filterableAttributes" := List(
          "productType",
          "variants.filterField",
          "variants.prices.currency",
          "variants.prices.centAmount"
        ),
        "sortableAttributes" := List(
          "variants.prices.centAmount"
        )
      )
    )
  )

  override def toElasticModel(
      model: Product
  ): Seq[ElasticsearchModel.ElasticDoc] =
    given Encoder[(PriceSelector, PriceDescriptor)] =
      Encoder { case (sel, price) =>
        Json.obj(
          "selector" := s"${sel.currency}_${sel.country
              .getOrElse("*")}_${sel.channel.getOrElse("*")}",
          "centAmount" := price.centAmount,
          "discountedCentAmount" := price.discountedCentAmount
        )
      }

    given Encoder[ProductVariant] = Encoder { v =>
      Json.obj(
        "sku" := v.sku,
        "name" := v.name,
        "filterField" := v.filterField,
        "prices" := v.prices.toList
      )
    }

    List(
      ElasticsearchModel.ElasticDoc(
        "products",
        model.id,
        Json.obj(
          "id" := model.id,
          "productType" := model.productType,
          "variants" := model.variants
        )
      )
    )

  override def query(query: ProductQuery): DomainModelMapping.ElasticQuery =
    DomainModelMapping.SingleElasticQuery(
      "products",
      Json.obj(
        "attributesToRetrieve" := List("id"),
        "filter" := List(
          List(
            s"productType IN [${query.productType.mkString(",")}]",
            s"variants.filterField = -1",
            s"variants.prices.centAmount >= ${query.centAmountRange.start}",
            s"variants.prices.centAmount < ${query.centAmountRange.end}",
            s"variants.prices.centAmount = 42"
          )
        ),
        "sort" := List(
          "variants.prices.centAmount:asc"
        )
      )
    )
