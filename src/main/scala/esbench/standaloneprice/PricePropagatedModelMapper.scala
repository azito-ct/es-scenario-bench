package esbench.standaloneprice

import io.circe._
import io.circe.syntax._
import esbench.DomainModelMapper
import esbench.ElasticsearchModel
import esbench.DomainModelMapping

object PricePropagatedModelMapper extends StandalonePriceBaseModelMapper:
  override def esMappings: Map[String, Json] = Map(
    "prices" -> Json.obj(
      "dynamic" := "false",
      "properties" := Json.obj(
        "productId" := Json.obj("type" := "keyword"),
        "productType" := Json.obj("type" := "keyword"),
        "variantSku" := Json.obj("type" := "keyword"),
        "variantName" := Json.obj("type" := "text"),
        "variantFilterField" := Json.obj("type" := "long"),
        "priceId" := Json.obj("type" := "keyword"),
        "priceSelector" := Json.obj("type" := "text"),
        "priceCentAmount" := Json.obj("type" := "long"),
        "priceDiscountedCentAmount" := Json.obj("type" := "long")
      )
    )
  )

  override def toElasticModel(
      model: Product
  ): Seq[ElasticsearchModel.ElasticDoc] =
    model.variants.flatMap { v =>
      v.prices.map { case (sel, price) =>
        ElasticsearchModel.ElasticDoc(
          "prices",
          price.id,
          Json.obj(
            "productId" := model.id,
            "productType" := model.productType,
            "variantSku" := v.sku,
            "variantName" := v.name,
            "variantFilterField" := v.filterField,
            "priceId" := price.id,
            "priceSelector" := s"${sel.currency}_${sel.country
                .getOrElse("*")}_${sel.channel.getOrElse("*")}",
            "priceCentAmount" := price.centAmount,
            "priceDiscountedCentAmount" := price.discountedCentAmount
          )
        )
      }
    }

  override def query(query: ProductQuery): DomainModelMapping.ElasticQuery =
    DomainModelMapping.SingleElasticQuery(
      "prices",
      Json.obj(
        "query" := Json.obj(
          "bool" := Json.obj(
            "filter" := List(
              Json.obj(
                "terms" := Json.obj("productType" := query.productType.toList)
              ),
              Json.obj(
                "term" := Json.obj("variantFilterField" := query.variantFilterField)
              ),
              Json.obj(
                "range" := Json.obj(
                  "priceCentAmount" := Json.obj(
                    "gte" := query.centAmountRange.start,
                    "lte" := query.centAmountRange.end
                  )
                )
              )
            )
          )
        )
      )
    )
