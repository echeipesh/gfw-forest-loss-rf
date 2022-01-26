package org.globalforestwatch
import geotrellis.vector._
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.proj4._
import org.apache.spark.sql.SparkSession
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.functions._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.datasource.geojson._

object TreeCoverLoss {

  import org.apache.spark.sql.functions.{col, udf, not => sql_not, when, sum}
  def spatialKeyFromExtent = udf { (e: Extent) => blockTileGrid.mapTransform.pointToKey(e.center) }

  def dataframe(implicit spark: SparkSession) = {
    import spark.implicits._

    val tclUri = "s3://gfw-data-lake/umd_tree_cover_loss/v1.8/raster/epsg-4326/10/40000/year/gdal-geotiff/10N_080E.tif"
    val tcdUri = "s3://gfw-data-lake/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/10/40000/percent/gdal-geotiff/10N_080E.tif"
    val catalog = List((tclUri, tcdUri)).toDF("tcl", "tcd")

    spark.read.raster
      .fromCatalog(catalog, "tcl", "tcd")
      .withTileDimensions(400, 400)
      .load()
      .select(spatialKeyFromExtent(col("tcl.extent")) as "spatial_key", col("tcl.tile") as "tcl_tile", col("tcd.tile") as "tcd_tile")
  }
  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  val blockTileGrid: LayoutDefinition = {
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
