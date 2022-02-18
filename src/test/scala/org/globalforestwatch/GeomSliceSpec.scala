package org.globalforestwatch

import org.locationtech.rasterframes.datasource.geojson._
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.raster.TileLayout
import geotrellis.raster.ByteCellType
import geotrellis.proj4.LatLng
import org.apache.spark.sql.functions.{col, udf, explode}

class GeomSliceSpec extends TestEnvironment {
  import spark.implicits._

  val locations = {
    val geom_uri = "/opt/data/gfwpro/verified/indonesia.geojson"
    spark.read.geojson.load(geom_uri.toString)
  }

  it("turns clips geometry to grid") {
    val gom = udf{  geom: Geometry => Locations.geomByGrid(geom, GridData.blockTileGrid) }
    val out = locations.
      withColumn("cell", explode(gom(col("geometry")))).
      select(col("list_id"), col("location_id"), col("cell._1") as "spatial_key", col("cell._2") as "geometry")

    out.printSchema()
    out.show()
    info(s"count: ${out.count}")
  }
}

object GridData {
  def gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
  def blockTileGrid: LayoutDefinition = {
    // copy from ForestChangeDiagnosticGrid
    val gridSize = 10
    val pixelSize = 0.00025
    val blockSize = 400
    val blocksPerGrid = (math.round(gridSize / pixelSize).toInt / blockSize)
    val tileLayout = TileLayout(
      layoutCols = (gridExtent.xmin.toInt until gridExtent.xmax.toInt by gridSize).length * blocksPerGrid,
      layoutRows = (gridExtent.ymin.toInt until gridExtent.ymax.toInt by gridSize).length * blocksPerGrid,
      tileCols = blockSize,
      tileRows = blockSize)
    LayoutDefinition(gridExtent, tileLayout)
  }

  val tlm = TileLayerMetadata(
    cellType = ByteCellType,
    layout = blockTileGrid,
    extent = gridExtent,
    crs = LatLng,
    bounds = KeyBounds(blockTileGrid.gridBoundsFor(gridExtent).toGridType[Int]))
}