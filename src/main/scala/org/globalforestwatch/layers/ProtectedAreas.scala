package org.globalforestwatch.layers

object ProtectedAreas extends RasterLayer {
  val name = "wdpa_protected_areas"

  def lookup(value: Int): String = value match {
    case 1 => "Category Ia/b or II"
    case 2 => "Other Category"
    case _ => ""
  }
}
