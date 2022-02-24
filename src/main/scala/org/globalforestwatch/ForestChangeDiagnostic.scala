package org.globalforestwatch

import cats.data.NonEmptyList
import com.monovore.decline._
import geotrellis.layer.SpatialKey
import geotrellis.vector._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, lit, udaf, udf, when}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.globalforestwatch.grids.TenByTen30mGrid
import org.globalforestwatch.layers._
import org.globalforestwatch.util.Geodesy
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



  def work(locationPaths: NonEmptyList[String])(implicit spark: SparkSession): Unit = {

    val grid = TenByTen30mGrid

    val udfPixelAreaHectares = udf { key: SpatialKey =>
      val layout = TenByTen30mGrid.segmentTileGrid
      val extent = key.extent(layout)
      val lat = (extent.ymax + extent.ymin) / 2
      Geodesy.pixelArea(lat, layout.cellSize) / 10000.0
    }

    val locations = spark.read.geojson.load(locationPaths.toList: _*)

    val locationMasks = Locations.locationsByGrid(locations, grid.segmentTileGrid)
      .withColumn("area", udfPixelAreaHectares(col("spatial_key")))
      .cache()

    val aoi: Geometry = locations
      .select(locations("geometry").as[Geometry])
      .where(locations("location_id") === -1)
      .first()

    val layers = List(
      TreeCoverLoss,
      TreeCoverDensityPercent2000,
      PrimaryForest,
      GFWProPeatlands,
      IntactForestLandscapes2000,
      ProtectedAreas,
      SEAsiaLandCover,
      IndonesiaLandCover,
      IndonesiaForestArea,
      IndonesiaForestMoratorium,
      ProdesLossYear,
      BrazilBiomes,
      PlantationsBool,
      GFWProCoverage
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

    // TODO: when layer.col is a bool layer, we can't mask it because it has no NODATA
    // 1. convert tile to int
    // 2. write a new function
    // 3. write a special case
    val maskedTileColumns: List[Column] = layers.map { layer =>
      //rf_mask_by_value(
      //  layer.col, col("mask"), lit(0)) as layer.name
      rf_local_multiply(layer.col, col("mask")) as layer.name
    }

    val pixels = flattened.select(
      col("list_id"),
      col("location_id"),
      col("area"),
      rf_explode_tiles(maskedTileColumns: _*)).cache

    pixels.printSchema()


    val analyses = List(
      Analysis.TreeCoverLossTotalYearly,
      Analysis.TreeCoverLoss90Yearly,
      Analysis.TreeCoverLossPrimaryForestYearly,
      Analysis.TreeCoverLossPeatYearly
    )

    val expanded = Analysis.addColumns(pixels, analyses)

    val df = expanded
      .groupBy(col("list_id"), col("location_id"))
      .agg(
        Analysis.TreeCoverLossTotalYearly.agg,
        Analysis.TreeCoverLoss90Yearly.agg,
        Analysis.TreeCoverLossPrimaryForestYearly.agg,
        Analysis.TreeCoverLossPeatYearly.agg
      )

    df.printSchema()
    df.repartition(1).write.mode(SaveMode.Overwrite).json("/tmp/gfw_out")
  }

}