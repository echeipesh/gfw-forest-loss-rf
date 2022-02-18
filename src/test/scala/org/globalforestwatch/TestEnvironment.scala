package org.globalforestwatch


import java.nio.file.{Files, Path}
import com.typesafe.scalalogging.Logger
import geotrellis.raster.testkit.RasterMatchers
import org.apache.spark.sql._
import org.apache.spark.{ SparkContext }
import org.scalactic.Tolerance
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import org.locationtech.rasterframes._

trait TestEnvironment extends AnyFunSpec
  with Matchers with Inspectors with Tolerance with RasterMatchers {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  lazy val scratchDir: Path = {
    val outputDir = Files.createTempDirectory("gfw-scratch-")
    outputDir.toFile.deleteOnExit()
    outputDir
  }

  def sparkMaster = "local[*, 2]"

  implicit lazy val spark: SparkSession = {
    val spark = SparkSession.builder().master("local[*]").
      appName("RasterFrames").
      withKryoSerialization.
      getOrCreate().withRasterFrames
      spark.sparkContext.setLogLevel("ERROR")
      spark
    }
  implicit def sparkContext: SparkContext = spark.sparkContext
}