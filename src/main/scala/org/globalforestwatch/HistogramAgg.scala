package org.globalforestwatch

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object HistogramAgg extends Aggregator[Array[Double], Array[Double], Map[Int, Double]] {
  def zero: Array[Double] = Array.fill(32)(0)

  def reduce(buffer: Array[Double], hist: Array[Double]): Array[Double] = {
    merge(buffer, hist)
  }

  def merge(b1: Array[Double], b2: Array[Double]): Array[Double] = {
    for ( i <- 0 until b1.length) {
      b1(i) = b1(i) + b2(i)
    }
    b1
  }

  implicit def typedEncoder[T: TypedEncoder]: ExpressionEncoder[T] =
    TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  // Transform the output of the reduction
  def finish(reduction: Array[Double]): Map[Int, Double] = reduction.zipWithIndex.map{ case (v, i) => (i, v)}.toMap
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Array[Double]] = typedEncoder[Array[Double]]
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Map[Int, Double]] = typedEncoder[Map[Int, Double]]
}
