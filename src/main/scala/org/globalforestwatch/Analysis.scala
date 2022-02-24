package org.globalforestwatch

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, explode, lit, udaf, udf, when}
import org.globalforestwatch.layers._

abstract class Analysis(val name: String) {
  def condition: Column
  def agg: Column
}

object Analysis {
  val aggByYear = udaf(LossYearAgg)

  def addColumns(df: DataFrame, analyses: List[Analysis]): DataFrame = {
    analyses.foldLeft(df){ (result, analysis) =>
      result.withColumn(analysis.name, analysis.condition)
    }
  }

  def aggColumns(analyses: List[Analysis]): List[Column] = {
    analyses.map(a => a.agg as a.name)
  }

  object TreeCoverLossTotalYearly extends Analysis("tree_cover_loss_total_yearly") {
    def condition: Column =
      when(TreeCoverLoss.col > 0 && TreeCoverDensityPercent2000.col > 30, col("area")) as name
    def agg: Column =
      Analysis.aggByYear(TreeCoverLoss.col as "loss_year", col(name) as "area") as name
  }

  object TreeCoverLoss90Yearly extends Analysis("tree_cover_loss_tcd90_yearly") {
    def condition: Column =
      when(TreeCoverLoss.col > 0 && TreeCoverDensityPercent2000.col > 90, col("area")) as name
    def agg: Column =
      Analysis.aggByYear(TreeCoverLoss.col as "loss_year", col(name) as "area") as name
  }

  object TreeCoverLossPrimaryForestYearly extends Analysis("tree_cover_loss_primary_forest_yearly") {
    def condition: Column =
      when(TreeCoverLoss.col > 0 && PrimaryForest.col > 0, col("area")) as name
    def agg: Column =
      Analysis.aggByYear(TreeCoverLoss.col as "loss_year", col(name) as "area") as name
  }

  object TreeCoverLossPeatYearly extends Analysis("tree_cover_loss_peat_yearly") {
    def condition: Column =
      when(TreeCoverLoss.col > 0 && GFWProPeatlands.col > 0, col("area")) as name
    def agg: Column =
      Analysis.aggByYear(TreeCoverLoss.col as "loss_year", col(name) as "area") as name
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

