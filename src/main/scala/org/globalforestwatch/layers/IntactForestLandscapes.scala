package org.globalforestwatch.layers

import org.globalforestwatch.grids.RasterLayerGrid
import org.globalforestwatch.config.GfwConfig

object IntactForestLandscapes extends RasterLayer {
  val name = "ifl_intact_forest_landscapes"

  def lookup(value: Int): String = value match {
    case 0 => ""
    case _ => value.toString

  }
}

object IntactForestLandscapes2000 extends RasterLayer {
  val name = "ifl_intact_forest_landscapes"

  def lookup(value: Int): Boolean = {
    value match {
      case 0 => false
      case _ => true
    }
  }
}

object IntactForestLandscapes2013 extends RasterLayer {
  val name = "ifl_intact_forest_landscapes"

  def lookup(value: Int): Boolean = {
    value match {
      case 2016 => true
      case 2013 => true
      case _ => false
    }
  }
}

object IntactForestLandscapes2016 extends RasterLayer {
  val name = "ifl_intact_forest_landscapes"

  def lookup(value: Int): Boolean = {
    value match {
      case 2016 => true
      case _ => false
    }
  }

}
