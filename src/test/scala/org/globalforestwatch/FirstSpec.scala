package org.globalforestwatch

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.datasource.geojson._
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.{ functions => fn }
import java.net.URI
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame


class FirstSpec extends TestEnvironment {
  import spark.implicits._

  lazy val rasters: DataFrame = ???/// TreeCoverLoss.dataframe(spark)

  it("uses tile max") {
    rasters.
      withColumn("tcd_max", rf_tile_max(rasters("tcd_tile"))).
      withColumn("tcl_max", rf_tile_max(rasters("tcl_tile"))).
      show(Int.MaxValue)
  }

  lazy val pixels = {
    rasters.select(
      rf_explode_tiles(
        rasters("tcd_tile") as "tcd",
        rasters("tcl_tile") as "tcl"))
      .withColumn("tcd", col("tcd").cast(IntegerType))
      .withColumn("tcl", when(col("tcl").isNaN, null).otherwise(col("tcl") + 2000).cast(IntegerType))
      .where(col("tcl").isNotNull)
  }

  it("makes pixels"){
    pixels.printSchema()
    pixels.show()
  }

  it("aggregates by year") {
    val pixels = List(
      (1, 2020, 91, 12),
      (1, 2020, 98, 1),
      (1, 2012, 10, 11)
    ).toDF("id", "loss_year", "tcd", "area")

    val step1 =
    pixels
    .withColumn("tcl_90", when(pixels("tcd") > 90, fn.map(pixels("loss_year"), pixels("area"))).otherwise(null))
    .withColumn("tcl_30", when(pixels("tcd") < 30, fn.map(pixels("loss_year"), pixels("area"))).otherwise(null))

    step1.show()

    step1
      .groupBy(pixels("id"))
      .agg(fn.map_concat(col("tcl_90")))
      .show()
  }

  it("aggregates by year by bool") {
    val pixels = List(
      (1, 2020, 91, 12),
      (1, 2020, 98, 1),
      (1, 2012, 10, 11)
    ).toDF("id", "loss_year", "tcd", "area")

    val step1 =
    pixels
    .withColumn("tcl_90_area", when(pixels("tcd") > 90, 'area).otherwise(null))
    .withColumn("tcl_30_area", when(pixels("tcd") < 30, 'area).otherwise(null))

    step1.show()

    val myagg = fn.udaf(LossYearAgg)

    step1
    .groupBy('id)
    .agg(
      myagg(col("loss_year"), col("tcl_90_area") as "area") as "tcl_90_yearly",
      myagg(col("loss_year"), col("tcl_30_area") as "area") as "tcl_30_yearly",
      myagg(col("loss_year"), col("landcover"), col("tcl_30_area") as "area") as "tcl_30_yearly"
    )
    .show(false)
  }

  val locations = {
    val geom_uri = "/opt/data/gfwpro/verified/indonesia.geojson"
    spark.read.geojson.load(geom_uri.toString)
  }

  ignore("asdf"){
    val uris = OldMain.expand(new URI("s3://gfw-data-lake/umd_tree_cover_loss/v1.8/raster/epsg-4326/10/40000/year/gdal-geotiff/"))
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
}