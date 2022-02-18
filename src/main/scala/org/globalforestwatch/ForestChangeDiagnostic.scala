package org.globalforestwatch

import cats.data.NonEmptyList
import com.monovore.decline._
import geotrellis.vector._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, lit, udaf, udf, when}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.globalforestwatch.grids.TenByTen30mGrid
import org.globalforestwatch.layers._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.geojson._

object ForestChangeDiagnostic extends SparkCommand  {

  val command: Opts[Unit] = Opts.subcommand(
    name = "fcd",
    help = "Compute summary statistics for GFW Pro Forest Change Diagnostic."
  ) {
    (
      Opts.options[String]("locations", help = "Path pointing to data that will be read")
    ).map { (locations) =>
      withSpark { implicit spark =>
        work(locations)
      }
    }
  }

  def keyForTile: UserDefinedFunction = udf { (e: Extent) =>
    TenByTen30mGrid.segmentTileGrid.mapTransform.pointToKey(e.center)
  }

  def work(locationPaths: NonEmptyList[String])(implicit spark: SparkSession): Unit = {

    val grid = TenByTen30mGrid

    val locations = spark.read.geojson.load(locationPaths.toList: _*)
    val locationMasks = Locations.locationsByGrid(locations, grid.segmentTileGrid)
      .withColumn("area", )
      .cache()

    val aoi = locations.select(locations("geometry").as[Geometry]).where(locations("location_id") === -1).first()

    val layers = List(
      TreeCoverLoss,
      TreeCoverDensityPercent2000
    )

    // I want to be able to give a list of case classes but also refer to them
    val joined = RasterLayer.joinLayers(locationMasks, grid, aoi, layers)


    // I will now explode
    val flattened = joined.select(
      col("*") ,
      explode(col("geom_cells"))
    )
    .withColumnRenamed("key", "location_id")
    .withColumnRenamed("value", "mask")

    val maskedTileColumns: List[Column] = layers.map { layer =>
      rf_mask(layer.col, col("mask")) as layer.name
    }

    val pixels = flattened.select(
      col("list_id"),
      col("location_id"),
      rf_explode_tiles(maskedTileColumns: _*),
      lit(1) as "area")

    val expanded = pixels
      .withColumn("tcl_90_area", when(pixels(TreeCoverDensityPercent2000.name) > 90, 'area).otherwise(null))
      .withColumn("tcl_30_area", when(pixels(TreeCoverDensityPercent2000.name) < 30, 'area).otherwise(null))


    val yearHistogram = udaf(LossYearAgg)

    val df = expanded.groupBy(col("list_id"), col("location_id"))
      .agg(
        yearHistogram(TreeCoverLoss.col as "loss_year", col("tcl_90_area") as "area") as "tcl_90_yearly",
        yearHistogram(TreeCoverLoss.col as "loss_year", col("tcl_30_area") as "area") as "tcl_30_yearly"
      )

    df.printSchema()
    df.repartition(1).write.mode(SaveMode.Overwrite).json("/tmp/gfw_out")
  }

}