package ru.ahub.interview

case class MatchResult(
    manufacturer: String,
    productionCountry: Option[String],
    brandName: String,
    modelCode: String,
    modelName: String,
    subBrand: Option[String],
    // transmission
    transmissionType: Option[String],
    transmissionDetailedType: Option[String],
    gearsCount: Option[Int],
    // engine
    engineCapacity: Option[Float],
    enginePowerW: Option[Short],
    fuel: Option[String],
    // body
    bodyType: Option[String],
    seatsCount: Option[Int],
    doorsCount: Option[Short],
    canInstallHitch: Option[Boolean],
    hasHitch: Option[Boolean],
    // stats
    mileage: Option[Long],
    minPrice: Option[Long],
    maxPrice: Option[Long],
    // features
    hasAirConditioner: Option[Boolean],
    hasStartStopSystem: Option[Boolean],
    hasDriverAssistant: Option[Boolean],
    hasABS: Option[Boolean])

case class Source1(
    brandName: String,
    modelCode: String,
    modelName: String,
    transmissionType: String,
    bodyType: String,
    seatsCount: Int,
    doorsCount: Short,
    mileage: Option[Long],
    minPrice: Option[Long],
    maxPrice: Option[Long],
    hasAirConditioner: Boolean,
    hasStartStopSystem: Boolean,
    hasDriverAssistant: Boolean,
    hasABS: Boolean)

case class Source2(
    modelCode: String,
    transmissionType: String,
    transmissionDetailedType: String,
    gearsCount: Int,
    fuel: String,
    price: Long,
    mileage: Option[Long],
    minPrice: Option[Long],
    maxPrice: Option[Long])

case class Source3(
    modelCode: String,
    transmissionType: String,
    transmissionDetailedType: String,
    gearsCount: Int,
    engineCapacity: Float,
    enginePowerW: Short,
    fuel: String,
    mileage: Option[Long],
    minPrice: Option[Long],
    maxPrice: Option[Long])

case class Source4(
    manufacturer: String,
    productionCountry: String,
    brandName: String,
    modelCode: String,
    modelName: String,
    subBrand: String,
    bodyType: String,
    seatsCount: Int,
    doorsCount: Short,
    canInstallHitch: Boolean,
    hasHitch: Boolean,
    mileage: Option[Long],
    minPrice: Option[Long],
    maxPrice: Option[Long])
