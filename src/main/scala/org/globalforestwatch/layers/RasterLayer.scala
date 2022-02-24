package org.globalforestwatch.layers

import geotrellis.vector._
import org.apache.spark.sql.{Column, DataFrame, functions => f}
import org.globalforestwatch.grids._
import org.globalforestwatch.config.GfwConfig
import org.locationtech.rasterframes._



trait RasterLayer {
  def name: String
  def col: Column = f.col(name)

  def resolve(grid: RasterLayerGrid): RasterLayerInstance = {
    val uriTemplate = GfwConfig.get.rasterCatalog.getSourceUri(name)
    RasterLayerInstance(this, grid, uriTemplate)
  }
}

object RasterLayer {
  def joinLayers(
    df: DataFrame,
    grid: RasterLayerGrid,
    aoi: Geometry,
    layers: List[RasterLayer]
  ): DataFrame = {
    def udfGetKey = f.udf { (e: Extent) =>
        grid.segmentTileGrid.mapTransform.pointToKey(e.center)
    }

    /** Assumes df is keyed by the same grid as parameter */
    val joined: DataFrame =
      layers.foldLeft(df) { (result, layer) =>
        val extentCol = f.col(s"${layer.name}.extent")
        val tiles: DataFrame =
          layer
            .resolve(grid)
            .query(aoi)(df.sparkSession)
            .withColumn("spatial_key", udfGetKey(extentCol))

        result.join(tiles, List("spatial_key"), joinType="left")
      }

    /* After join replace lazy raster references with materialized tiles */
    val materialized: DataFrame =
      layers.foldLeft(joined) { (result, layer) =>
        result.withColumn(layer.name, rf_tile(f.col(layer.name)))
      }

    materialized
  }
}