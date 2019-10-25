package com.relatedsciences.opentargets.pipeline
import scala.io.Source

object Utilities {
  /**
   * Resource manager wrapper
   * @param r resource to close after use
   * @param f function to apply to resource
   */
  def using[A](r: Source)(f: Source => A): A = {try f(r) finally r.close()}
}
