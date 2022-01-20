package org.globalforestwatch

import geotrellis.layer._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.polygonal.visitors._
import geotrellis.spark._

import cats.implicits._
import com.monovore.decline._

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.geojson._
import org.locationtech.rasterframes.datasource.raster._
import org.log4s._

import java.net.URI
import scala.collection.JavaConversions._

object  Main {
  @transient private[this] lazy val logger = getLogger

  private val inputsOpt = Opts.options[URI](
    "input", help = "Path pointing to data that will be read")
  private val geomOpt = Opts.option[URI](
    "geom", help = "URI of geometry data for join")
  private val outputOpt = Opts.option[String](
    "output", help = "The path of the output tiffs")
  private val partitionsOpt =  Opts.option[Int](
    "numPartitions", help = "The number of partitions to use").orNone

  private val command = Command(name = "gfw-forest-loss-rf", header = "GeoTrellis App: gfw-forest-loss-rf") {
    (inputsOpt, geomOpt, outputOpt, partitionsOpt).tupled
  }

  def main(args: Array[String]): Unit = {
    command.parse(args, sys.env) match {
      case Left(help) =>
        System.err.println(help)
        sys.exit(1)

      case Right((i, g, o, p)) =>
        try {
          run(i.toList, g, o, p)(Spark.session)
        } finally {
          Spark.session.stop()
        }
    }
  }

  def run(inputs: List[URI], geom_uri: URI, output: String, numPartitions: Option[Int])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // Job Logic
    val allInputs = inputs.map(expand(_))
    logger.warn(s"Found ${allInputs.map(_.length)} inputs")

    val toKey = udf { p => pathToKey(p) }
    val df = allInputs
      .map(spark.read.raster.from(_).load.select(toKey('proj_raster_path) as 'key, 'proj_raster))
      .zipWithIndex
      .map{ case (df, i) => df.withColumnRenamed("proj_raster", s"proj_raster_$i") }
      .reduce{ (a, b) =>
        a.alias("a").join(b.alias("b"), a("key") === b("key"), "outer").drop(col("b.key"))
      }

    df.printSchema

    // Key geometries to layout
    val geomToKey = udf { g: Geometry =>
      layout.mapTransform.keysForGeometry(g).toSeq
    }

    val geom = spark.read.geojson.load(geom_uri.toString)
    val keyedGeom = geom.select('geometry, explode(geomToKey('geometry)) as 'key, 'location_id as 'id)
    keyedGeom.printSchema

    // Join to df
    val withGeom = df.alias("a")
      .join(keyedGeom.alias("b"), df("key") === keyedGeom("key"), "inner")
      .drop(col("b.key"))

    withGeom.printSchema

    // Use polygonalsummary to create histograms per intersection
    val summaryByKey = withGeom.map(summarizeRow)

    // Group by polygonal id
    // Merge summaries



  }

  case class Summarized(id: String, histograms: Seq[StreamingHistogram])

  def summarizeRow(row: Row): Summarized = {
    val rasterFields = row.schema.filter(_.name.startsWith("proj_raster_"))
    val rasterIxs = rasterFields.map(row.schema.fields.indexOf(_)).toSeq
    val geom = row.getAs[Geometry]("geometry")
    val histos = for (ix <- rasterIxs) yield {
      val raster_row = row.getStruct(ix)
      val tile = raster_row.getAs[Tile]("tile")
      val extent_row = raster_row.getStruct(1)
      val extent = Extent(extent_row.getDouble(0), extent_row.getDouble(1), extent_row.getDouble(2), extent_row.getDouble(3))
      val raster = Raster(tile, extent)

      raster.polygonalSummary(geom, StreamingHistogramVisitor) match {
        case Summary(h) => h
      }
    }

    Summarized(row.getAs[String]("id"), histos)
  }

  def expand(uri: URI): Seq[URI] = {
    uri.getScheme match {
      case "s3" =>
        val s3Client = _root_.geotrellis.store.s3.S3ClientProducer.get.apply
        val req = software.amazon.awssdk.services.s3.model.ListObjectsV2Request
          .builder
          .bucket(uri.getHost)
          .prefix(uri.getPath.drop(1))
          .requestPayer("requester")
          .build
        val resp = s3Client.listObjectsV2(req)
        resp
          .contents
          .filter(_.key.endsWith(".tif"))
          .map{ obj =>
            new URI(s"s3://${uri.getHost}/${obj.key}")
          }
      case x if (x == "http" || x == "https") =>
        logger.error("HTTP(S) sources not supported")
        Seq()
      case x if (x == null || x == "file")  =>
        val d = if (x == "file")
                  new java.io.File(uri.toURL.getFile)
                else
                  new java.io.File(uri.toString)
        if (d.exists) {
          if (d.isDirectory) {
            d.listFiles.map(f => new URI(f.toString))
          } else {
            Seq(uri)
          }
        } else {
          logger.warn(s"Provided source (uri) was not directory or file")
          Seq()
        }
    }
  }

  def pathToKey(path: String): SpatialKey = {
    val ix = raw"(\d+)([NS])_(\d+)([EW]).tif".r
    val (dx, dy) = path.split("/").last match {
      case ix(y, ns, x, ew) => (if (ew=="E") x.toInt else -x.toInt, if (ns=="N") y.toInt else -y.toInt)
    }
    SpatialKey(dx / 10 + 18, 8 - dy / 10)
  }

  val layout = LayoutDefinition(RasterExtent(Extent(-180, -90, 180, 90), 360, 180), 10, 10)
}
