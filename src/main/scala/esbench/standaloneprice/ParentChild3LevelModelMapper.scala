package esbench.standaloneprice

import io.circe._
import io.circe.syntax._
import esbench.ElasticsearchModel
import esbench.DomainModelMapping

object ParentChild3LevelModelMapper extends StandalonePriceBaseModelMapper:
  override def esMappings: Map[String, Json] = Map(
    "products" -> Json.obj(
      "dynamic" := "false",
      "properties" := Json.obj(
        "type" := Json.obj(
          "type" := "join",
          "relations" := Json.obj(
            "product" := "variant",
            "variant" := "price"
          )
        ),
        "productId" := Json.obj("type" := "keyword"),
        "productType" := Json.obj("type" := "keyword"),
        "variantNr" := Json.obj("type" := "long"),
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
    val productDoc = ElasticsearchModel.ElasticDoc(
      "products",
      model.id,
      Json.obj(
        "type" := "product",
        "productId" := model.id,
        "productType" := model.productType
      )
    )

    val variantsDocs = model.variants.zipWithIndex.map { case (variant, idx) =>
      ElasticsearchModel.ElasticDoc(
        "products",
        s"${model.id}#$idx",
        Json.obj(
          "type" := Json.obj(
            "name" := "variant",
            "parent" := model.id
          ),
          "variantNr" := idx,
          "variantSku" := variant.sku,
          "variantName" := variant.name,
          "variantFilterField" := variant.filterField
        ),
        routing = Some(model.id)
      )
    }

    val pricesDocs = model.variants.zipWithIndex
      .flatMap { case (variant, idx) => variant.prices.map(_ -> idx) }
      .map { case ((sel, price), idx) =>
        ElasticsearchModel.ElasticDoc(
          "products",
          s"${model.id}$$${price.id}",
          Json.obj(
            "type" := Json.obj(
              "name" := "price",
              "parent" := s"${model.id}#${idx}"
            ),
            "priceId" := price.id,
            "priceSelector" := s"${sel.currency}_${sel.country
                .getOrElse("*")}_${sel.channel.getOrElse("*")}",
            "priceCentAmount" := price.centAmount,
            "priceDiscountedCentAmount" := price.discountedCentAmount
          ),
          routing = Some(model.id)
        )
      }

    productDoc :: variantsDocs ++ pricesDocs

  override def query(query: ProductQuery): DomainModelMapping.ElasticQuery =
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
                "has_child" := Json.obj(
                  "type" := "variant",
                  "query" := Json.obj(
                    "bool" := Json.obj(
                      "filter" := List(
                        Json.obj(
                          "term" := Json.obj(
                            "variantFilterField" := query.variantFilterField
                          )
                        ),
                        Json.obj(
                          "has_child" := Json.obj(
                            "type" := "price",
                            "query" := Json.obj(
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
                )
              )
            )
          )
        )
      )
    )
