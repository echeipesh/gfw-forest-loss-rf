package org.globalforestwatch.util

import java.net.URI

object FileUtils {
  def s3PrefixExists(uri: String): Boolean = {
    val obj = URI.create(uri)
    val s3Client = geotrellis.store.s3.S3ClientProducer.get.apply
    val req = software.amazon.awssdk.services.s3.model.ListObjectsV2Request
      .builder
      .bucket(obj.getHost)
      .prefix(obj.getPath.drop(1))
      .requestPayer("requester")
      .build
    val resp = s3Client.listObjectsV2(req)
    !resp.contents.isEmpty
  }
}
