package org.globalforestwatch

import org.apache.spark.sql.{Encoder}
import org.apache.spark.sql.expressions.Aggregator
import cats.implicits._
import frameless._
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class LossArea(loss_year: Int, area: Option[Double])

object LossYearAgg extends Aggregator[LossArea, Map[Int, Double], Map[Int, Double]] {
  def zero: Map[Int, Double] = Map.empty

  def reduce(buffer: Map[Int, Double], employee: LossArea): Map[Int, Double] = {
    employee.area.map { a =>
      buffer.get(employee.loss_year) match {
        case Some(v) => buffer.updated(employee.loss_year, a + v)
        case None => Map(employee.loss_year -> a)
      }
    }.getOrElse(buffer)
  }

  def merge(b1: Map[Int, Double], b2: Map[Int, Double]): Map[Int, Double] = {
    b1 |+| b2
  }

  implicit def typedEncoder[T: TypedEncoder]: ExpressionEncoder[T] =
    TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  // Transform the output of the reduction
  def finish(reduction: Map[Int, Double]): Map[Int, Double] = reduction.toMap
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Map[Int, Double]] = typedEncoder[Map[Int, Double]]
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Map[Int, Double]] = typedEncoder[Map[Int, Double]]
}
