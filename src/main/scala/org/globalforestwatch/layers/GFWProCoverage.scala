package org.globalforestwatch.layers


object GFWProCoverage extends RasterLayer {

  val name = "gfwpro_forest_change_regions"

  def lookup(value: Int): Map[String, Boolean] = {
    val bits = "0000000" + value.toBinaryString takeRight 8
    Map(
      "South America" -> (bits(6) == '1'),
      "Legal Amazon" -> (bits(5) == '1'),
      "Brazil Biomes" -> (bits(4) == '1'),
      "Cerrado Biomes" -> (bits(3) == '1'),
      "South East Asia" -> (bits(2) == '1'),
      "Indonesia" -> (bits(1) == '1')
    )
  }
}
