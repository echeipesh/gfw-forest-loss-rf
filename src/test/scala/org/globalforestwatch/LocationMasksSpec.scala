package org.globalforestwatch

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.datasource.geojson._
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.raster._
import org.apache.hadoop.hive.ql.parse.HiveParser.blocking_return
import geotrellis.raster.ByteCellType
import geotrellis.proj4.LatLng
import org.apache.spark.sql.functions.{col, udf, explode,collect_list, struct}
import org.apache.spark.sql.{functions => fn}
import java.net.URI

case class Bucket(location_id: Long, geometry: Geometry)

/** I need to create a cell/masks DF for all locations */
class LocationMasksSpec extends TestEnvironment {
  import spark.implicits._

  val locations = {
    val geom_uri = "/opt/data/gfwpro/verified/indonesia.geojson"
    spark.read.geojson.load(geom_uri.toString)
  }

  it("turns clips geometry to grid") {
    val gom = udf{  geom: Geometry => GeomSlicer.sliceGeomByGrid(geom, GridData.blockTileGrid) }
    val locTiles = locations.
      withColumn("cell", explode(gom(col("geometry")))).
      select(col("list_id"), col("location_id"), col("cell._1") as "spatial_key", col("cell._2") as "geometry")//.groupBy("list_id", "spatial_key").pivot("location_id").count()

    locTiles.printSchema()
    // out.show()


    val locGroups = locTiles.groupBy("list_id", "spatial_key").agg(fn.map_from_entries(collect_list(struct(locTiles("location_id"), locTiles("geometry")))) as "geom_list")
    locGroups.printSchema()
    locGroups.show()

    val udfCheck = udf { (tcl: Tile, tcd: Tile, geoms: Array[Bucket]) =>
      print(s"$tcl, $tcd, ${geoms.length}")
      geoms.length
      // return histograms
    }


    val rasters = TreeCoverLoss.dataframe(spark)
    val joined = locGroups.join(rasters, rasters("spatial_key") === locGroups("spatial_key"), "left")
      .where(rasters("tcl_tile").isNotNull)

    joined.show()
    // joined.select(rasters("spatial_key"), udfCheck(col("tcl_tile"), col("tcd_tile"), col("geom_list").as[Array[Bucket]])).show()
    // joined.select(rasters("spatial_key"), udfCheck(col("tcl_tile"), col("tcd_tile"), col("geom_list").as[Array[Bucket]])).show()
  }

}
