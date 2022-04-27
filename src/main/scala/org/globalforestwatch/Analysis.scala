package org.globalforestwatch

import geotrellis.layer.SpatialKey
import geotrellis.raster.{Raster, Tile}
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster.summary.polygonal.PolygonalSummary
import geotrellis.vector.Geometry
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, udaf, udf, when}
import org.globalforestwatch.grids._
import org.globalforestwatch.layers._
import org.locationtech.rasterframes.{rf_local_greater, rf_local_multiply}

abstract class Analysis(val name: String) {
  def content: Column
  def agg: Column =
    Analysis.aggByYear(TreeCoverLoss.col as "loss_year", col(name) as "area") as name
}

abstract class HistogramAnalysis(override val name: String) extends Analysis(name) {
  def content: Column =
    Analysis.scaleHistogram(
      histogram,
      col("area")
    ) as name
  def criterionTile: Column
  def histogram: Column =
      Analysis.histogramByYear(
        col("spatial_key"),
        col("geometry"),
        TreeCoverLoss.col,
        criterionTile
      )
  override def agg: Column =
      Analysis.aggHist(col(name)) as name
}

object Analysis {
  val aggByYear = udaf(LossYearAgg)
  val aggHist = udaf(HistogramAgg)

  def addColumns(df: DataFrame, analyses: List[Analysis]): DataFrame = {
    analyses.foldLeft(df){ (result, analysis) =>
      result.withColumn(analysis.name, analysis.content)
    }
  }

  def aggColumns(analyses: List[Analysis]): List[Column] = {
    analyses.map(a => a.agg as a.name)
  }

  // ---------------------------------------------------- Pixelwise Aggregations
  object TreeCoverLossTotalYearlyPW extends Analysis("tree_cover_loss_total_yearly") {
    def content: Column =
      when(TreeCoverLoss.col > 0 && TreeCoverDensityPercent2000.col > 30, col("area")) as name
  }

  object TreeCoverLoss90YearlyPW extends Analysis("tree_cover_loss_tcd90_yearly") {
    def content: Column =
      when(TreeCoverLoss.col > 0 && TreeCoverDensityPercent2000.col > 90, col("area")) as name
  }

  object TreeCoverLossPrimaryForestYearlyPW extends Analysis("tree_cover_loss_primary_forest_yearly") {
    def content: Column =
      when(TreeCoverLoss.col > 0 && PrimaryForest.col > 0, col("area")) as name
  }

  object TreeCoverLossPeatYearlyPW extends Analysis("tree_cover_loss_peat_yearly") {
    def content: Column =
      when(TreeCoverLoss.col > 0 && GFWProPeatlands.col > 0, col("area")) as name
  }


  // --------------------------------------------------- Rasterized Aggregations
  class HistogramVisitor(val result: Array[Double], key: Tile) extends GridVisitor[Raster[Tile], Array[Double]] {
    def visit(data: Raster[Tile], c: Int, r: Int) = {
      val ix = key.get(c, r)
      if (ix >= 0)
        result(ix) += data.tile.getDouble(c, r)
    }
  }

  def rasterizeToHistogram(
    key: SpatialKey,
    geom: Geometry,
    yearTile: Tile,
    criterionTile: Tile
  ): Array[Double] = {
    val trans = TenByTen30mGrid.segmentTileGrid.mapTransform
    val extent = trans.keyToExtent(key)
    val year = Raster(yearTile, extent)
    val data = Raster(criterionTile, extent)
    val zero: Array[Double] = Array.fill(32)(0)
    val visitor = new HistogramVisitor(zero, year.tile)
    PolygonalSummary(data, geom, visitor, PolygonalSummary.DefaultOptions)
      .toOption
      .getOrElse(zero)
  }

  val histogramByYear = udf { (key, geom, year, criterion) =>
    rasterizeToHistogram(key, geom, year, criterion)
  }

  def histogramScale(hist: Array[Double], scale: Double) = hist.map(_ * scale)

  val scaleHistogram = udf { (hist, factor) => histogramScale(hist, factor) }

  object TreeCoverLossTotalYearly extends HistogramAnalysis("tree_cover_loss_total_yearly") {
    // when(TreeCoverLoss.col > 0 && TreeCoverDensityPercent2000.col > 30, col("area"))
    def criterionTile: Column = rf_local_multiply(
      rf_local_greater(
        TreeCoverLoss.col,
        0
      ),
      rf_local_greater(
        TreeCoverDensityPercent2000.col,
        30
      )
    )
  }

  object TreeCoverLoss90Yearly extends HistogramAnalysis("tree_cover_loss_tcd90_yearly") {
    //   when(TreeCoverLoss.col > 0 && TreeCoverDensityPercent2000.col > 90, col("area"))
    def criterionTile: Column = rf_local_multiply(
      rf_local_greater(
        TreeCoverLoss.col,
        0
      ),
      rf_local_greater(
        TreeCoverDensityPercent2000.col,
        90
      )
    )
  }

  object TreeCoverLossPrimaryForestYearly extends HistogramAnalysis("tree_cover_loss_primary_forest_yearly") {
    // when(TreeCoverLoss.col > 0 && PrimaryForest.col > 0, col("area"))
    def criterionTile: Column = rf_local_multiply(
      rf_local_greater(
        TreeCoverLoss.col,
        0
      ),
      rf_local_greater(
        PrimaryForest.col,
        0
      )
    )
  }

  object TreeCoverLossPeatYearly extends HistogramAnalysis("tree_cover_loss_peat_yearly") {
    // when(TreeCoverLoss.col > 0 && GFWProPeatlands.col > 0, col("area")) as name
    def criterionTile: Column = rf_local_multiply(
      rf_local_greater(
        TreeCoverLoss.col,
        0
      ),
      rf_local_greater(
        GFWProPeatlands.col,
        0
      )
    )
  }
  //
  //.withColumn("tree_cover_loss_intact_forest_yearly",
  //  when(
  //    pixels(TreeCoverLoss.name) > 0 and pixels(IntactForestLandscapes2000.name) > 0,
  //    'area))
  //  .withColumn("tree_cover_loss_protected_areas_yearly",
  //    when(
  //      pixels(TreeCoverLoss.name) > 0 and pixels(ProtectedAreas.name) > 0,
  //      'area))
  //  .withColumn("tree_cover_loss_soy_yearly",
  //    when(
  //      pixels(TreeCoverLoss.name) > 0 and pixels(SoyPlantedAreas.name) > 0,
  //      'area))
  //  .withColumn("tree_cover_extent_total",
  //    when(
  //      pixels(TreeCoverDensityPercent2000.name) > 30,
  //      'area))
  //  .withColumn("tree_cover_extent_primary_forest",
  //    when(
  //      pixels(TreeCoverDensityPercent2000.name) > 30 and pixels(PrimaryForest.name) > 0,
  //      'area))
  //  .withColumn("tree_cover_extent_protected_areas",
  //    when(
  //      pixels(TreeCoverDensityPercent2000.name) > 30 and pixels(ProtectedAreas.name) > 0,
  //      'area))
  //  .withColumn("tree_cover_extent_peat",
  //    when(
  //      pixels(TreeCoverDensityPercent2000.name) > 30 and pixels(GFWProPeatlands.name) > 0,
  //      'area))
  //  .withColumn("tree_cover_extent_intact_forest",
  //    when(
  //      pixels(TreeCoverDensityPercent2000.name) > 30 and pixels(IntactForestLandscapes2000.name) > 0,
  //      'area))
  //  .withColumn("natural_habitat_primary",
  //    when(pixels(PrimaryForest.name) > 0, 'area))
  //  .withColumn("natural_habitat_intact_forest",
  //    when(pixels(IntactForestLandscapes2000.name) > 0, 'area))
  //.withColumn("total_area",
  //  when(pixels(IntactForestLandscapes2000.name) > 0, 'area))


}
