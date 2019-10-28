package com.relatedsciences.opentargets.pipeline
import scala.io.Source

object Utilities {

  /**
    * Resource manager wrapper
    * @param r resource to close after use
    * @param f function to apply to resource
    */
  def using[A](r: Source)(f: Source => A): A = {
    try f(r)
    finally r.close()
  }

  def clip[T](v: T, rng: (T, T))(implicit n: Numeric[T]): T = {
    val x  = n.toDouble(v)
    val r1 = n.toDouble(rng._1)
    val r2 = n.toDouble(rng._2)
    if (x > r2) rng._2
    else if (x < r1) rng._1
    else v
  }

  /**
    * Project a value within one numeric domain to another
    *
    * For example, the value .5 in the domain [0, 1] may be projected into the domain [0, 100] where the
    * corresponding value is 50.
    *
    * @param v value to project
    * @param curRng current domain as (start, end)
    * @param newRng new domain as (start, end)
    * @return value in new domain
    * @throws IllegalArgumentException if start of either range is less than or equal to end of range
    */
  def normalize(v: Double, curRng: (Double, Double), newRng: (Double, Double)): Double = {
    require(curRng._2 > curRng._1)
    require(newRng._2 > newRng._1)
    val r = newRng._1 + ((v - curRng._1) / (curRng._2 - curRng._1)) * (newRng._2 - newRng._1)
    clip(r, newRng)
  }

  def scorePValue(pValue: Double, rng: (Double, Double) = (1e-10, 1.0)): Double = {
    val pValueLog = Math.log10(pValue)
    val rngLog    = (Math.log10(rng._1), Math.log10(rng._2))
    // Normalize log p value into 0-1 range and flip interpretation since higher p-values are
    // supposed to have lower scores (i.e. scorePValue(.0001) > scorePValue(.1))
    val pLinear = normalize(pValueLog, rngLog, (0.0, 1.0))
    assert(pLinear >= 0 && pLinear <= 1)
    1.0 - pLinear
  }

}
