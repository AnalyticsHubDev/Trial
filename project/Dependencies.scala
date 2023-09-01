import sbt._

object Dependencies {

  object Versions {
    val spark = "3.3.0"
    val zio = "2.0.16"
    val scalatest = "3.2.16"
    val scalamock = "5.2.0"
    val config = "1.4.1"
    val circe = "0.14.3"
    val kafkaClient = "2.2.2"
  }

  import Versions._

  val list: List[ModuleID] = List(
    "org.apache.spark" %% "spark-core" % spark % Provided,
    "org.apache.spark" %% "spark-sql" % spark % Provided,
    "org.apache.spark" %% "spark-hive" % spark % Provided)

}
