package org.globalforestwatch.config

import org.globalforestwatch.grids.RasterLayerGrid
import io.circe.generic.auto._
import io.circe.parser.decode
import scalaj.http.{Http, HttpOptions, HttpResponse}

case class RasterCatalog(layers: List[LayerConfig]) {
  def getSourceUri(dataset: String): String = {
    //  lyr.grid == s"${grid.gridSize}/${grid.rowCount}"
    layers.find(lyr => lyr.name == dataset) match {
      case Some(lyr: LayerConfig) => lyr.source_uri
      case None =>
        throw new IllegalArgumentException(s"No configuration found for dataset ${dataset}")
    }
  }
}

case class LayerConfig(name: String, source_uri: String)

object RasterCatalog {
  def getRasterCatalog(catalogFile: String): RasterCatalog = {
    val rawJson = scala.io.Source.fromResource(catalogFile).getLines.mkString

    val parsed = decode[RasterCatalog](rawJson) match {
      case Left(exc) =>
        println(s"Invalid data environment json: ${rawJson}")
        throw exc
      case Right(value) => value
    }

    RasterCatalog(
      parsed.layers.map((config: LayerConfig) =>
        LayerConfig(
          config.name,
          getSourceUri(config.name, config.source_uri)
        )
      )
    )
  }

  def getSourceUri(dataset: String, sourceUri: String): String = {
    if (sourceUri.contains("latest")) {
      sourceUri.replace("latest", getLatestVersion(dataset))
    } else {
      sourceUri
    }
  }

  def getLatestVersion(dataset: String): String = {
    val response: HttpResponse[String] = Http(
      s"https://data-api.globalforestwatch.org/dataset/${dataset}/latest"
    ).option(HttpOptions
      .followRedirects(true)).option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(50000)).asString

    if (response.code != 200)
      throw new IllegalArgumentException(
        s"Dataset ${dataset} has no latest version or does not exit. Data API response code: ${response.code}"
      )

    val json: Map[String, Any] = {
      import org.json4s.jackson.JsonMethods.parse
      implicit val formats = org.json4s.DefaultFormats
      parse(response.body).extract[Map[String, Any]]
    }

    val data = json.get("data").asInstanceOf[Option[Map[String, Any]]]
    data match {
      case Some(map) =>
        val version = map.get("version").asInstanceOf[Option[String]]
        version match {
          case Some(value) => value
          case _ => throw new RuntimeException("Cannot understand Data API response.")
        }
      case _ => throw new RuntimeException("Cannot understand Data API response.")
    }
  }
}