package com.relatedsciences.opentargets.pipeline
import org.scalatest.FunSuite
import org.scalactic.{Equality, TolerantNumerics}

class UtilitiesSuite extends FunSuite {

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-16)

  def assertEqual(expected: Double)(actual: Double): Unit ={
    assert(expected === actual)
  }

  test("normalization works for valid cases") {
    assertEqual(.5 )(Utilities.normalize(1.5, (1, 2), (0, 1)))
    assertEqual(1.75)(Utilities.normalize(.75, (0, 1), (1, 2)))
    assertEqual(1)(Utilities.normalize(0, (0, 1), (1, 2)))
    assertEqual(2)(Utilities.normalize(1, (0, 1), (1, 2)))
    assertEqual(2)(Utilities.normalize(10, (0, 1), (1, 2)))
    assertEqual(1)(Utilities.normalize(-1, (0, 1), (1, 2)))

    // From https://github.com/opentargets/data_pipeline/blob/d82f4cfc1e92ab58f1ab5b5553a03742e808d9df/tests/test_score.py#L62
    assertEqual(0.6111111111111112)(Utilities.normalize(.2, (0.0, .9), (.5, 1.0)))
    assertEqual(1.0)(Utilities.normalize(2.0, (0.0, .9), (.5, 1.0)))
    assertEqual(0.5)(Utilities.normalize(-.2, (0.0, .9), (.5, 1.0)))
    assertEqual(9.000090000900009e-05)(Utilities.normalize(10, (1, 100000), (0, 1)))
    assertEqual(0)(Utilities.normalize(1, (1, 100000), (0, 1)))
    assertEqual(1)(Utilities.normalize(100005, (1, 100000), (0, 1)))
    assertEqual(.5)(Utilities.normalize(2500, (0, 5000), (0, 1)))
  }

  test("normalization argument checks on oob arguments") {
    assertThrows[IllegalArgumentException] {
      Utilities.normalize(.5, (1, 0), (1, 2))
    }
    assertThrows[IllegalArgumentException] {
      Utilities.normalize(.5, (1, 1), (1, 2))
    }
    assertThrows[IllegalArgumentException] {
      Utilities.normalize(.5, (1, 2), (3, 2))
    }
  }

  test("p value transformations") {
    // From https://github.com/opentargets/data_pipeline/blob/d82f4cfc1e92ab58f1ab5b5553a03742e808d9df/tests/test_score.py#L84
    assertEqual(0)(Utilities.scorePValue(1))
    assertEqual(0)(Utilities.scorePValue(10))
    assertEqual(1)(Utilities.scorePValue(1e-10))
    assertEqual(1)(Utilities.scorePValue(1e-30))
    assertEqual(.5)(Utilities.scorePValue(1e-5))
    assertEqual(0)(Utilities.scorePValue(1e-2, rng = (1e-10, 1e-2)))
    assertEqual(1)(Utilities.scorePValue(1e-10, rng = (1e-10, 1e-2)))
    assertEqual(0)(Utilities.scorePValue(1, rng = (1e-10, 1e-2)))
    assertEqual(.75)(Utilities.scorePValue(1e-5, rng = (1e-6, 1e-2)))
  }
}
