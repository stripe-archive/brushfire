package com.stripe.brushfire

package object features {
  type FeatureValue = Dispatched[Double, String, Double, String]
  type CsvRow = IndexedSeq[String]
}
