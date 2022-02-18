package org.globalforestwatch.layers

import org.globalforestwatch.grids.RasterLayerGrid
import org.globalforestwatch.config.GfwConfig

object Plantations extends RasterLayer {

  //TODO: switch over to gfw_planted_forests dataset which is the same, but registered with the API
  //  An issue here is that the resampled raster assets for viirs and modis currently would depend on the vector asset
  //  However, for this dataset we would want to resample the data based on the raster asset using mode resampling method.
  //  We will first need to update data-api to make this possible

  val name = "gfw_plantations"

  def lookup(value: Int): String = value match {
    case 1 => "Fruit"
    case 2 => "Fruit Mix"
    case 3 => "Oil Palm "
    case 4 => "Oil Palm Mix"
    case 5 => "Other"
    case 6 => "Rubber"
    case 7  => "Rubber Mix"
    case 8  => "Unknown"
    case 9  => "Unknown Mix"
    case 10 => "Wood fiber / Timber"
    case 11 => "Wood fiber / Timber Mix"
    case _ => ""
  }
}

object PlantationsBool extends RasterLayer {
  val name = "gfw_plantations"
}
