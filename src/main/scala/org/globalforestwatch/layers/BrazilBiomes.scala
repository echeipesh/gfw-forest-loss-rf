package org.globalforestwatch.layers

object BrazilBiomes extends RasterLayer {
  val name = "ibge_bra_biomes"
  val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "Caatinga"
    case 2 => "Cerrado"
    case 3 => "Pantanal"
    case 4 => "Pampa"
    case 5 => "Amazônia"
    case 6 => "Mata Atlântica"
    case _ => "Unknown"
  }
}
