package org.globalforestwatch.layers


trait TreeCoverDensityThreshold extends RasterLayer {

  val externalNoDataValue: Integer = 0

  def lookup(value: Int): Integer = {
    value match {
      case v if v <= 10 => 0
      case v if v <= 15 => 10
      case v if v <= 20 => 15
      case v if v <= 25 => 20
      case v if v <= 30 => 25
      case v if v <= 50 => 30
      case v if v <= 75 => 50
      case _ => 75
    }
  }
}

object TreeCoverDensityThreshold2000 extends TreeCoverDensityThreshold {
  val name = "umd_tree_cover_density_2000"
}

object TreeCoverDensityThreshold2010 extends TreeCoverDensityThreshold {
  val name = "umd_tree_cover_density_2010"
}

object TreeCoverDensity2010_60 extends RasterLayer {
  val name = "umd_tree_cover_density_2010"
  def lookup(value: Int): Boolean = value > 60
}

object TreeCoverDensityPercent2000 extends RasterLayer {
  val name = "umd_tree_cover_density_2000"
  val externalNoDataValue: Integer = 0
}

object TreeCoverDensityPercent2010 extends RasterLayer {
  val name = "umd_tree_cover_density_2010"
  val externalNoDataValue: Integer = 0
}
