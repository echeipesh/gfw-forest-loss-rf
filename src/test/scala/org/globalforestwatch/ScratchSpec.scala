package org.globalforestwatch

import geotrellis.raster._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.apache.spark.sql.functions.lit

class ScratchSpec extends TestEnvironment {

  import spark.implicits._

  it("runs") {
    val catalog = List((
      "s3://gfw-data-lake/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/10/40000/threshold/gdal-geotiff/00N_110E.tif",
      null: String
    )).toDF("tcd", "xxx")

    val rasters = spark.read.raster
      .fromCatalog(catalog, "tcd", "xxx")
      .withTileDimensions(400, 400)
      .load()

    rasters.printSchema()
    rasters.show()

    //val tiles = rasters.select(rasters("proj_raster.tile"))

    //tiles.withColumn("other", lit(null: Tile)).printSchema()

    //tiles.select(rf)
    //
    //val df = tiles.select(rf_explode_tiles(tiles("asdf")))
    //df.printSchema()
    //df.show()
  }

  it("rasters") {
    val tile: Tile = IntArrayTile.fill(1, 6, 6)
    val tiles = List((tile, null: Tile)).toDF("tcd", "xxx")

    val df = tiles.select(rf_explode_tiles(tiles("tcd"), tiles("xxx")))
    df.printSchema()
    df.show()
  }

}
