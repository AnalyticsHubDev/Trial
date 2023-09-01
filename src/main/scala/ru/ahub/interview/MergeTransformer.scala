package ru.ahub.interview

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.coalesce

class MergeTransformer(spark: SparkSession) {

  import spark.implicits._

  def merge(
      s1: Dataset[Source1],
      s2: Dataset[Source2],
      s3: Dataset[Source3],
      s4: Dataset[Source4]
  ): Unit = {
    s1.join(s2, s1("brandName") === s2("brandName") && s1("modelCode") === s2("modelCode"), "left")
        .join(s3, s1("brandName") === s3("brandName") && s1("modelCode") === s3("modelCode"), "left")
        .join(s4, s1("brandName") === s4("brandName") && s1("modelCode") === s4("modelCode"), "left")
        .transform(_.select(
          s4("manufacturer")                                                          as "manufacturer",
          s4("productionCountry")                                                     as "productionCountry",
          s1("brandName")                                                             as "brandName",
          s1("modelCode")                                                             as "modelCode",
          s1("modelName")                                                             as "modelName",
          s1("transmissionType")                                                      as "transmissionType",
          coalesce(s2("transmissionDetailedType"), s3("transmissionDetailedType"))    as "transmissionDetailedType",
          s3("gearsCount")                                                            as "gearsCount",
          s3("engineCapacity")                                                        as "engineCapacity",
          s3("enginePowerW")                                                          as "enginePowerW",
          coalesce(s2("fuel"), s3("fuel"))                                            as "fuel",
          s1("bodyType")                                                              as "bodyType",
          s1("seatsCount")                                                            as "seatsCount",
          s1("doorsCount")                                                            as "doorsCount",
          s4("canInstallHitch")                                                       as "canInstallHitch",
          s4("hasHitch")                                                              as "hasHitch",
          coalesce(s1("mileage"), s2("mileage"), s3("mileage"), s4("mileage"))        as "mileage",
          coalesce(s1("minPrice"), s2("minPrice"), s3("minPrice"), s4("minPrice"))    as "minPrice",
          coalesce(s1("maxPrice"), s2("maxPrice"), s3("maxPrice"), s4("maxPrice"))    as "maxPrice",
          s1("hasAirConditioner")                                                     as "hasAirConditioner",
          s1("hasStartStopSystem")                                                    as "hasStartStopSystem",
          s1("hasDriverAssistant")                                                    as "hasDriverAssistant",
          s1("hasABS")                                                                as "hasABS"))
        .write
        .saveAsTable("default.result_table")
  }

}
