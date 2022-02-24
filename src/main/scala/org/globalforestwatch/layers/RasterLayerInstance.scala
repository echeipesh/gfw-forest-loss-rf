package org.globalforestwatch.layers

import geotrellis.layer._
import geotrellis.vector._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.rasterframes.{geomLit, rf_geometry, st_intersects}
import org.locationtech.rasterframes.datasource.raster._
import org.apache.spark.sql.{functions => F}
import org.globalforestwatch.grids._
import org.globalforestwatch.util.FileUtils

import java.net.URI

case class RasterLayerInstance(
  layer: RasterLayer,
  grid: RasterLayerGrid,
  uriTemplate: String
) {
  def uri(key: SpatialKey): String = {
    val layout = grid.rasterFileGrid
    val extent = layout.mapTransform.keyToExtent(key)
    val col = math.floor(extent.xmin).toInt
    val row = math.ceil(extent.ymax).toInt
    val lng: String = if (col >= 0) f"$col%03dE" else f"${-col}%03dW"
    val lat: String = if (row >= 0) f"$row%02dN" else f"${-row}%02dS"

    val tileId = s"${lat}_${lng}"

    uriTemplate
      .replace("{grid_size}", grid.gridSize.toString)
      .replace("{row_count}", grid.rowCount.toString)
      .replace("{tile_id}", tileId)

  }

  private def load(uris: List[String])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val catalog = uris.toDF(layer.name)
    spark.read
      .raster
      .fromCatalog(catalog, layer.name)
      .withTileDimensions(grid.blockSize, grid.blockSize)
      .withLazyTiles(true)
      .load()
  }

  def list(aoi: Geometry): List[String] = {
    grid.rasterFileGrid.mapTransform.keysForGeometry(aoi).toList
      .map { key => uri(key) }
      .filter { uri =>
        FileUtils.s3PrefixExists(uri)
      }
  }

  def query(aoi: Geometry)(implicit spark: SparkSession): DataFrame = {
    val uris = list(aoi)
    // Filtering for only the blocks that intersect our AOI actually reduces the dataset dramatically
    load(uris).where(st_intersects(rf_geometry(F.col(layer.name)), geomLit(aoi)))
  }

}
