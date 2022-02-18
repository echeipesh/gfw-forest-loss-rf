package org.globalforestwatch

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.datasource.geojson._
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.raster._
import org.apache.hadoop.hive.ql.parse.HiveParser.blocking_return
import geotrellis.raster.ByteCellType
import geotrellis.proj4.LatLng
import org.apache.spark.sql.types.{IntegerType, BooleanType}
import org.apache.spark.sql.functions.{col, udf, explode,collect_list, struct}
import org.apache.spark.sql.{functions => fn}
import org.apache.spark.sql.DataFrame
import java.net.URI

case class Bucket(location_id: Long, geometry: Geometry)

/** I need to create a cell/masks DF for all locations */
class LocationMasksSpec extends TestEnvironment {
  import spark.implicits._

  val locations = {
    val geom_uri = "/opt/data/gfwpro/verified/indonesia.geojson"
    spark.read.geojson.load(geom_uri.toString)
  }

  def tileGeomByGrid = udf {  geom: Geometry => Locations.geomByGrid(geom, GridData.blockTileGrid) }

  def locGroups = locations
    .withColumn("cell", explode(tileGeomByGrid(col("geometry"))))
    .select(col("list_id"), col("location_id"), col("cell._1") as "spatial_key", col("cell._2") as "geometry")
    .groupBy("list_id", "spatial_key")
    .agg(fn.map_from_entries(
      collect_list(
        struct(
          col("location_id"),
          col("geometry")
          // rf_rasterize(col("geometry"),col("geometry"), fn.lit(1), 400, 400)
        )
      )
    ) as "geom_cells")

  def locCells = locations
    .withColumn("cell", explode(tileGeomByGrid(col("geometry"))))
    .select(col("list_id"), col("location_id"), col("cell._1") as "spatial_key", col("cell._2") as "geometry")

  ignore("clip geoms by grid") {
    locGroups.printSchema()
    locGroups.show()
  }

  ignore("explodes raster pixels") {
    println("--explodes raster pixels")
    val rasters: DataFrame = ???///TreeCoverLoss.dataframe(spark).withColumnRenamed("spatial_key", "raster_key")

    val pixels =
      rasters.select(
        rf_explode_tiles(
          rasters("tcd_tile") as "tcd",
          rasters("tcl_tile") as "tcl"))
        .withColumn("tcd", col("tcd").cast(IntegerType))
        .withColumn("tcl", fn.when(col("tcl").isNaN, null).otherwise(col("tcl") + 2000).cast(IntegerType))
        .where(col("tcl").isNotNull)
    pixels.printSchema()
    pixels.show()
  }

  ignore("joins clipped geoms with rasters"){
    val rasters: DataFrame = ???///TreeCoverLoss.dataframe(spark).withColumnRenamed("spatial_key", "raster_key")
    // val rasters = TreeCoverLoss.dataframe(spark).withColumnRenamed("spatial_key", "raster_key")
    val locations = locGroups.withColumnRenamed("spatial_key", "location_key")

    val joined = locations.join(rasters, 'location_key === 'raster_key, "left")
      .where(rasters("tcl_tile").isNotNull)
    joined.show()
  }

  it("explodes cells"){
    // val rasters = TreeCoverLoss.dataframe(spark).withColumnRenamed("spatial_key", "raster_key")
    val rasters: DataFrame = ???///TreeCoverLoss.dataframe(spark).withColumnRenamed("spatial_key", "raster_key")
    val locations = locCells.withColumnRenamed("spatial_key", "location_key")

    val joined = locations.join(rasters, 'location_key === 'raster_key, "left")

    val pixels = joined
    .withColumn("mask", rf_rasterize(col("geometry"),col("geometry"), fn.lit(1), 400, 400))
    .select(
      locations("list_id"),
      locations("location_id"),
      rf_explode_tiles(
        rf_mask(rasters("tcl_tile"), col("mask")) as "tcl",
        rf_mask(rasters("tcd_tile"), col("mask")) as "tcd"
      )
    )

    // pixels.show()

    info(s"pixel count: ${pixels.count()}")
  }

}
