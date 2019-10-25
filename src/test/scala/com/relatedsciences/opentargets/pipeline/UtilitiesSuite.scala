package com.relatedsciences.opentargets.pipeline
import org.scalatest.FunSuite

class UtilitiesSuite extends FunSuite {

  test("normalization works for valid cases") {
    assert(Utilities.normalize(1.5, (1, 2), (0, 1)) == .5)
    assert(Utilities.normalize(.75, (0, 1), (1, 2)) == 1.75)
    assert(Utilities.normalize(0, (0, 1), (1, 2)) == 1)
    assert(Utilities.normalize(1, (0, 1), (1, 2)) == 2)
    assert(Utilities.normalize(10, (0, 1), (1, 2)) == 2)
    assert(Utilities.normalize(-1, (0, 1), (1, 2)) == 1)
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
}
