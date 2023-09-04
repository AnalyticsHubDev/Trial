package ru.ahub.interview

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first, lit, max, min, struct}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class MergeTransformer(spark: SparkSession) {

  import spark.implicits._

  def merge(
      s1: Dataset[Source1],
      s2: Dataset[Source2],
      s3: Dataset[Source3],
      s4: Dataset[Source4]
  ): Unit = {
    val unifiedS1: Dataset[MatchResult] = s1.transform(selectAs[Source1, MatchResult])
    val unifiedS2: Dataset[MatchResult] = s2.transform(selectAs[Source2, MatchResult])
    val unifiedS3: Dataset[MatchResult] = s3.transform(selectAs[Source3, MatchResult])
    val unifiedS4: Dataset[MatchResult] = s4.transform(selectAs[Source4, MatchResult])
    val spec = Window.partitionBy($"v.brandName", $"v.modelCode").orderBy("priority")

    Seq(unifiedS1, unifiedS2, unifiedS3, unifiedS4)
        .map(_.transform(df => df.select(struct(df("*")) as "v", lit(1) as "priority")))
        .reduce(_ union _)
        .select(
          first("v.manufacturer", ignoreNulls = true).over(spec)              as "manufacturer",
          first("v.productionCountry", ignoreNulls = true).over(spec)         as "productionCountry",
          first("v.brandName", ignoreNulls = true).over(spec)                 as "brandName",
          first("v.modelCode", ignoreNulls = true).over(spec)                 as "modelCode",
          first("v.modelName", ignoreNulls = true).over(spec)                 as "modelName",
          first("v.subBrand", ignoreNulls = true).over(spec)                  as "subBrand",
          first("v.transmissionType", ignoreNulls = true).over(spec)          as "transmissionType",
          first("v.transmissionDetailedType", ignoreNulls = true).over(spec)  as "transmissionDetailedType",
          first("v.gearsCount", ignoreNulls = true).over(spec)                as "gearsCount",
          first("v.engineCapacity", ignoreNulls = true).over(spec)            as "engineCapacity",
          first("v.enginePowerW", ignoreNulls = true).over(spec)              as "enginePowerW",
          first("v.fuel", ignoreNulls = true).over(spec)                      as "fuel",
          first("v.bodyType", ignoreNulls = true).over(spec)                  as "bodyType",
          first("v.seatsCount", ignoreNulls = true).over(spec)                as "seatsCount",
          first("v.doorsCount", ignoreNulls = true).over(spec)                as "doorsCount",
          first("v.canInstallHitch", ignoreNulls = true).over(spec)           as "canInstallHitch",
          first("v.hasHitch", ignoreNulls = true).over(spec)                  as "hasHitch",
          first("v.mileage", ignoreNulls = true).over(spec)                   as "mileage",
          min("v.minPrice").over(spec)                                        as "minPrice",
          max("v.maxPrice").over(spec)                                        as "maxPrice",
          first("v.hasAirConditioner", ignoreNulls = true).over(spec)         as "hasAirConditioner",
          first("v.hasStartStopSystem", ignoreNulls = true).over(spec)        as "hasStartStopSystem",
          first("v.hasDriverAssistant", ignoreNulls = true).over(spec)        as "hasDriverAssistant",
          first("v.hasABS", ignoreNulls = true).over(spec)                    as "hasABS")
        .write
        .saveAsTable("default.result_table")
  }

  private def selectAs[Source <: Product : TypeTag, Result <: Product : TypeTag](ds: Dataset[Source]): Dataset[Result] = {
    val resultSchema = Seq.empty[Result].toDS().schema
    val sourceSchema = ds.columns.map(_.toLowerCase())

    val projection = resultSchema.map { f =>
      val colName = f.name.toLowerCase()
      if (sourceSchema.contains(colName)) col(colName) else lit(null).cast(f.dataType).as(colName)
    }

    ds.select(projection: _*).as[Result]
  }

}
