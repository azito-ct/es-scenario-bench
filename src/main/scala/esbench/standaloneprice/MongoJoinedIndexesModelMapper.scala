package esbench.standaloneprice

import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import esbench.ElasticsearchModel
import esbench.DomainModelMapping

object MongoJoinedIndexesModelMapper extends StandalonePriceBaseModelMapper:
  override def esMappings: Map[String, Json] = Map(
    "products" -> Json.arr(
      Json.obj("sku" := 1),
      Json.obj("productType" := 1),
      Json.obj("variantFilterField" := 1),
    ),
    "prices" -> Json.arr(
      Json.obj("sku" := 1),
      Json.obj("centAmount" := 1),
    )
  )

  override def toElasticModel(
      model: Product
  ): Seq[ElasticsearchModel.ElasticDoc] =
    val productDocs = model.variants.map { v =>
      ElasticsearchModel.ElasticDoc(
        "products",
        v.sku,
        Json.obj(
          "productId" := model.id,
          "productType" := model.productType,
          "sku" := v.sku,
          "variantName" := v.name,
          "variantFilterField" := v.filterField
        )
      )

    }

    val pricesDocs = model.variants.flatMap { v =>
      v.prices.map { case (sel, price) =>
        ElasticsearchModel.ElasticDoc(
          "prices",
          price.id,
          Json.obj(
            "sku" := v.sku,
            "currency" := sel.currency,
            "country" := sel.country,
            "channel" := sel.channel,
            "centAmount" := price.centAmount,
            "discountedCentAmount" := price.discountedCentAmount
          )
        )
      }
    }

    productDocs ++ pricesDocs

  override def query(query: ProductQuery): DomainModelMapping.ElasticQuery =
    DomainModelMapping.SingleElasticQuery(
      "products",
      Json.arr(
        Json.obj(
          "$match" := Json.obj(
            "productType" := Json.obj("$in" := query.productType),
            "variantFilterField" := query.variantFilterField
          )
        ),
        Json.obj(
          "$lookup" := Json.obj(
            "from" := "prices",
            "localField" := "sku",
            "foreignField" := "sku",
            "as" := "prices"
          )
        ),
        Json.obj(
          "$project" := Json.obj(
            "prices" := Json.obj(
              "$filter" := Json.obj(
                "input" := "$prices",
                "as" := "price",
                "cond" := Json.obj(
                  "$and" := List(
                    Json.obj(
                      "$gt" := List(
                        "$$price.centAmount".asJson,
                        query.centAmountRange.start.asJson
                      )
                    ),
                    Json.obj(
                      "$lt" := List(
                        "$$price.centAmount".asJson,
                        query.centAmountRange.`end`.asJson
                      )
                    )
                  )
                )
              )
            )
          )
        ),
        Json.obj(
          "$match" := Json.obj(
            "prices" := Json.obj("$not" := Json.obj("$size" := 0))
          )
        ),
        Json.obj(
          "$limit" := 100
        )
      )
    )
