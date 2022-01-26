package org.globalforestwatch

import javax.jdo.annotations.Column
import geotrellis.layer._
import geotrellis.vector._
import org.locationtech.jts.geom.prep.PreparedGeometry
import org.locationtech.jts.geom.prep.PreparedGeometryFactory

object GeomSlicer {
  def sliceGeomByGrid(geom: Geometry, layout: LayoutDefinition): Seq[(SpatialKey, Geometry)] = {
    val keys = layout.mapTransform.keysForGeometry(geom)
    val pg = PreparedGeometryFactory.prepare(geom)
    keys.toSeq.map { key =>
      val keyExtent = key.extent(layout).toPolygon()
      val part = geom.intersection(keyExtent)
      if (part.getNumPoints() > 1250) print(s"MANY POINTS: ${part.getNumPoints}")
      (key, part)
    }
  }




}