name := "gfw-forest-loss-rf"
organization := "org.globalforestwatch"
version := "0.1.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.monovore" %% "decline" % "1.2.0",
  "org.locationtech.rasterframes" %% "rasterframes" % "0.10.0",
  "org.locationtech.rasterframes" %% "rasterframes-datasource" % "0.10.0",
  "org.apache.spark" %% "spark-core" % "3.1.2" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.1.2" % Provided,
  "org.apache.spark" %% "spark-hive" % "3.1.2" % Provided
)
resolvers ++= Seq(
  "LT-releases" at "https://repo.locationtech.org/content/groups/releases",
  "LT-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
  "OSGeo" at "https://repo.osgeo.org/repository/release/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "jitpack" at "https://jitpack.io"
)

console / initialCommands :=
"""
import java.net._
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.gdal._
import geotrellis.spark._
import org.globalforestwatch._
""".stripMargin

// Fork JVM for test context to avoid memory leaks in Metaspace
Test / fork := true
Test / outputStrategy := Some(StdoutOutput)

// Settings for sbt-assembly plugin which builds fat jars for spark-submit
assembly / assemblyMergeStrategy := {
  case "reference.conf"   => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case ("MANIFEST.MF" :: Nil) =>
        MergeStrategy.discard
      case ("services" :: _ :: Nil) =>
        MergeStrategy.concat
      case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case (name :: Nil) if name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF") =>
        MergeStrategy.discard
      case _ =>
        MergeStrategy.first
      }
  case _ => MergeStrategy.first
}

// Settings from sbt-lighter plugin that will automate creating and submitting this job to EMR
import sbtlighter._

sparkEmrRelease := "emr-6.4.0"
sparkAwsRegion := "us-east-1"
sparkClusterName := "gfw-forest-loss-rf"
sparkEmrApplications := Seq("Spark", "Zeppelin", "Ganglia")
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("geotrellis-emr")
sparkS3JarFolder := "s3://geotrellis-test/jobs/jars"
sparkS3LogUri := Some("s3://geotrellis-test/jobs/logs")
sparkMasterType := "m4.xlarge"
sparkCoreType := "m4.xlarge"
sparkInstanceCount := 5
sparkMasterPrice := Some(0.5)
sparkCorePrice := Some(0.5)
sparkEmrServiceRole := "EMR_DefaultRole"
sparkInstanceRole := "EMR_EC2_DefaultRole"
sparkMasterEbsSize := Some(64)
sparkCoreEbsSize := Some(64)
sparkEmrBootstrap := List(
  BootstrapAction(
    "Install GDAL",
    "s3://geotrellis-demo/emr/bootstrap/conda-gdal.sh",
    "3.1.2"
  )
)
sparkEmrConfigs := List(
  EmrConfig("spark").withProperties(
    "maximizeResourceAllocation" -> "true"
  ),
  EmrConfig("spark-defaults").withProperties(
    "spark.driver.maxResultSize" -> "3G",
    "spark.dynamicAllocation.enabled" -> "true",
    "spark.shuffle.service.enabled" -> "true",
    "spark.shuffle.compress" -> "true",
    "spark.shuffle.spill.compress" -> "true",
    "spark.rdd.compress" -> "true",
    "spark.driver.extraJavaOptions" ->"-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",
    "spark.executor.extraJavaOptions" -> "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",
    "spark.yarn.appMasterEnv.LD_LIBRARY_PATH" -> "/usr/local/miniconda/lib/:/usr/local/lib",
    "spark.executorEnv.LD_LIBRARY_PATH" -> "/usr/local/miniconda/lib/:/usr/local/lib"
  ),
  EmrConfig("yarn-site").withProperties(
    "yarn.resourcemanager.am.max-attempts" -> "1",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false"
  )
)
