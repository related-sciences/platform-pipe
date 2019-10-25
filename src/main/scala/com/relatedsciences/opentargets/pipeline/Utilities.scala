package com.relatedsciences.opentargets.pipeline
import scala.io.Source

object Utilities {
  /**
   * Resource manager wrapper
   * @param r resource to close after use
   * @param f function to apply to resource
   */
  def using[A](r: Source)(f: Source => A): A = {try f(r) finally r.close()}

  def clip[T](v: T, rng: (T, T))(implicit n: Numeric[T]): T = {
    val x = n.toDouble(v)
    val r1 = n.toDouble(rng._1)
    val r2 = n.toDouble(rng._2)
    if (x > r2) rng._2
    else if (x < r1) rng._1
    else v
  }

  def normalize(v: Double, curRng: (Double, Double), newRng: (Double, Double)): Double = {
    require(curRng._2 > curRng._1)
    require(newRng._2 > newRng._1)
    val r = newRng._1 + ((v - curRng._1) / (curRng._2 - curRng._1)) * (newRng._2 - newRng._1)
    clip(r, newRng)
  }

}
