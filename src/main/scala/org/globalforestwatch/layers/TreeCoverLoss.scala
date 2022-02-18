package org.globalforestwatch.layers

object TreeCoverLoss extends RasterLayer {
  val name = "umd_tree_cover_loss"

  def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
