package org.globalforestwatch.layers


object ProdesLossYear extends RasterLayer {
  val name = "inpe_prodes"

  def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
