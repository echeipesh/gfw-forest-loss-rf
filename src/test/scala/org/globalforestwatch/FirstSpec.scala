package org.globalforestwatch

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.functions._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.datasource.geojson._
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.raster.TileLayout
import org.apache.hadoop.hive.ql.parse.HiveParser.blocking_return
import geotrellis.raster.ByteCellType
import geotrellis.proj4.LatLng
import org.apache.spark.sql.functions.{col, udf, not => sql_not, when, sum}
import java.net.URI
import org.apache.spark.sql.types.IntegerType

class FirstSpec extends TestEnvironment {
  import spark.implicits._

  val rasters = TreeCoverLoss.dataframe(spark)

  val pixels = {
    rasters.select(
      rf_explode_tiles(
        col("tcd.tile") as "tcd",
        col("tcl.tile") as "tcl"))
      .withColumn("tcd", col("tcd").cast(IntegerType))
      .withColumn("tcl", when(col("tcl").isNaN, null).otherwise(col("tcl") + 2000).cast(IntegerType))
      .where(col("tcl").isNotNull)
  }

  ignore("uses tile max") {
    rasters.
      withColumn("tcd_max", rf_tile_max(col("tcd.tile"))).
      withColumn("tcl_max", rf_tile_max(col("tcl.tile"))).
      show(Int.MaxValue)
  }

  ignore("aggregates by year") {
    pixels.
      where(col("tcd") > 30).
      groupBy(col("tcl")).
      agg(sum(col("tcd")))
      .show()

  }
  val locations = {
    val geom_uri = "/opt/data/gfwpro/verified/indonesia.geojson"
    spark.read.geojson.load(geom_uri.toString)
  }

  ignore("asdf"){
    val uris = Main.expand(new URI("s3://gfw-data-lake/umd_tree_cover_loss/v1.8/raster/epsg-4326/10/40000/year/gdal-geotiff/"))
    val df = spark.read.raster
      .from(uris)
      .withTileDimensions(400, 400)
      .load()

    df.printSchema()
    val out =  df.select(rf_explode_tiles(col("proj_raster.tile")))
    out.printSchema()
    out.show()
    // info(s"count: ${df.count()}")
  }

  ignore("turns extent to geometry") {
    rasters.select(st_geometry(col("tcl.extent"))).show()
  }

  ignore("joins rasters") {
    val joined = locations.join(rasters, st_intersects(st_geometry(col("tcl.extent")), col("geometry")))
    joined.printSchema()
    joined.show()
  }

  ignore("key rasters") {
    // import org.apache.spark.sql.functions.{col, udf}
    // val showExtent = udf{ (e: Extent) => Data.blockTileGrid.mapTransform.pointToKey(e.center) }
    // val keyed = rasters.withColumn("spatial_key", showExtent(col("tcl.extent")))
    // info("Is Layer:" + keyed.isAlreadyLayer)
    // keyed.show(truncate = false)
  }

  ignore("reads geom") {
    locations.show()
  }
}