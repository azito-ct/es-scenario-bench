package esbench.standaloneprice

import io.circe._
import io.circe.syntax._

import cats.implicits._

import esbench.DomainModelMapper
import esbench.ElasticsearchModel
import esbench.DomainModelMapping

object NestedModelMapper extends StandalonePriceBaseModelMapper:
  override def esMappings: Map[String, Json] = Map(
    "products" -> Json.obj(
      "dynamic" := "false",
      "properties" := Json.obj(
        "id" := Json.obj("type" := "keyword"),
        "productType" := Json.obj("type" := "keyword"),
        "variants" := Json.obj(
          "type" := "nested",
          "properties" := Json.obj(
            "sku" := Json.obj("type" := "keyword"),
            "name" := Json.obj("type" := "text"),
            "filterField" := Json.obj("type" := "long"),
            "prices" := Json.obj(
              "type" := "nested",
              "properties" := Json.obj(
                "selector" := Json.obj("type" := "text"),
                "centAmount" := Json.obj("type" := "long"),
                "discountedCentAmount" := Json.obj("type" := "long")
              )
            )
          )
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

  override def query(
      query: ProductQuery
  ): DomainModelMapping.ElasticQuery =
    DomainModelMapping.SingleElasticQuery(
      "products",
      Json.obj(
        "query" := Json.obj(
          "bool" := Json.obj(
            "filter" := List(
              Json.obj(
                "terms" := Json.obj("productType" := query.productType.toList)
              ),
              Json.obj(
                "nested" := Json.obj(
                  "path" := "variants",
                  "query" := Json.obj(
                    "bool" := Json.obj(
                      "filter" := List(
                        Json.obj(
                          "term" := Json.obj("variants.filterField" := query.variantFilterField)
                        ),
                        Json.obj(
                          "nested" := Json.obj(
                            "path" := "variants.prices",
                            "query" := Json.obj(
                              "range" := Json.obj(
                                "variants.prices.centAmount" := Json.obj(
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
                )
              )
            )
          )
        )
      )
    )
