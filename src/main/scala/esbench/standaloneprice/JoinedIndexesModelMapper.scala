package esbench.standaloneprice

import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import esbench.ElasticsearchModel
import esbench.DomainModelMapping

object JoinedIndexesModelMapper extends StandalonePriceBaseModelMapper:
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
            "filterField" := Json.obj("type" := "long")
          )
        )
      )
    ),
    "prices" -> Json.obj(
      "dynamic" := "false",
      "properties" := Json.obj(
        "priceId" := Json.obj("type" := "keyword"),
        "sku" := Json.obj("type" := "keyword"),
        "selector" := Json.obj("type" := "text"),
        "centAmount" := Json.obj("type" := "long"),
        "discountedCentAmount" := Json.obj("type" := "long")
      )
    )
  )

  override def toElasticModel(
      model: Product
  ): Seq[ElasticsearchModel.ElasticDoc] =
    given Encoder[ProductVariant] = Encoder { v =>
      Json.obj(
        "sku" := v.sku,
        "name" := v.name,
        "filterField" := v.filterField
      )
    }

    val productDoc = ElasticsearchModel.ElasticDoc(
      "products",
      model.id,
      Json.obj(
        "id" := model.id,
        "productType" := model.productType,
        "variants" := model.variants
      )
    )

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

    productDoc :: pricesDocs

  override def query(query: ProductQuery): DomainModelMapping.ElasticQuery =
    val productsQuery = DomainModelMapping.SingleElasticQuery(
      "products",
      Json.obj(
        "size" := 1000,
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
                    "term" := Json.obj(
                      "variants.filterField" := query.variantFilterField
                    )
                  ),
                  "inner_hits" := Json.obj(
                    "name" := "variants",
                    "size" := 100
                  )
                )
              )
            )
          )
        )
      )
    )

    case class VariantSource(sku: String)
    case class VariantHit(_source: VariantSource)
    case class VariantInnerHitsHits(hits: List[VariantHit])
    case class VariantInnerHits(hits: VariantInnerHitsHits)
    case class InnerHits(variants: VariantInnerHits)
    def pricesQuery(
        productHits: ElasticsearchModel.SearchHits
    ): DomainModelMapping.SingleElasticQuery =
      val skus = productHits.hits.flatMap { h =>
        h.inner_hits match {
          case Some(innerHitsJson) =>
            val innerHits = innerHitsJson.as[InnerHits].fold(throw _, identity)

            innerHits.variants.hits.hits.map(_._source.sku)

          case None => Nil
        }
      }
      
      DomainModelMapping.SingleElasticQuery(
        "prices",
        Json.obj(
          "size" := 1000,
          "query" := Json.obj(
            "bool" := Json.obj(
              "filter" := List(
                Json.obj(
                  "terms" := Json.obj("sku" := skus)
                ),
                Json.obj(
                  "range" := Json.obj(
                    "centAmount" := Json.obj(
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

    DomainModelMapping.SequencedElasticQuery(productsQuery, List(pricesQuery))
