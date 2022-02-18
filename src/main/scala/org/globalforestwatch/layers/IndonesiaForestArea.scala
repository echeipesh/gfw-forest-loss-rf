package org.globalforestwatch.layers


object IndonesiaForestArea extends RasterLayer {

  val name = "idn_forest_area"

  val externalNoDataValue: String = ""

  def lookup(value: Int): String = value match {
    case 1 => "Protected Forest"
    case 2 => "Production Forest"
    case 3 => "Limited Production Forest"
    case 4 => "Converted Production Forest"
    case 5 => "Other Utilization Area"
    case 6 => "Sanctuary Reserves/Nature Conservation Area"
    case 7 => "Marine Protected Areas"
    case _ => ""

  }
}
