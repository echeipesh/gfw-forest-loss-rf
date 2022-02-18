package org.globalforestwatch

import geotrellis.layer._
import geotrellis.vector._
import org.locationtech.jts.geom.prep.PreparedGeometryFactory
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.datasource.geojson._
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.raster._
import org.apache.hadoop.hive.ql.parse.HiveParser.blocking_return
import org.apache.spark.sql.DataFrame
import geotrellis.raster.ByteCellType
import geotrellis.proj4.LatLng
import org.apache.spark.sql.functions.{col, collect_list, explode, struct, udf}
import org.apache.spark.sql.{functions => fn}

import java.net.URI
import org.apache.spark.sql.SparkSession

object Locations {
  case class Cell(spatial_key: SpatialKey, geometry: Geometry)

  def geomByGrid(geom: Geometry, layout: LayoutDefinition): Seq[Cell] = {
    val keys = layout.mapTransform.keysForGeometry(geom)
    val pg = PreparedGeometryFactory.prepare(geom)
    keys.toSeq.map { key =>
      val keyExtent = key.extent(layout).toPolygon()
      val part = geom.intersection(keyExtent)
      if (part.getNumPoints() > 1250) println(s"MANY POINTS: ${part.getNumPoints}")
      Cell(key, part)
    }
  }

  def readGeoJson(uri: String, layout: LayoutDefinition)(implicit spark: SparkSession): DataFrame = {
    val tileGeomByGrid = udf {  geom: Geometry => Locations.geomByGrid(geom, layout) }
    spark.read.geojson.load(uri)
      .withColumn("cell", explode(tileGeomByGrid(col("geometry"))))
      .select(
        col("list_id"),
        col("location_id"),
        col("cell._1") as "spatial_key",
        col("cell._2") as "geometry")
      .groupBy("list_id", "spatial_key")
      .agg(fn.map_from_entries(collect_list(struct(col("location_id"), col("geometry")))) as "geom_cells")
  }

  def locationsByGrid(locations: DataFrame, grid: LayoutDefinition): DataFrame =  {
    val tileGeomByGrid = udf { geom: Geometry => geomByGrid(geom, grid) }

    locations
      .withColumn("cell", explode(tileGeomByGrid(locations("geometry"))))
      .drop("geometry")
      .select(locations("list_id"), locations("location_id"), col("cell.*"))
      .groupBy("list_id", "spatial_key")
      .agg(fn.map_from_entries(
        collect_list(
          struct(
            col("location_id"),
            //col("geometry")
            rf_rasterize(col("geometry"), col("geometry"), fn.lit(1), 400, 400)
          )
        )
      ) as "geom_cells")
  }

}