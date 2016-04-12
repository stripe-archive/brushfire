package com.stripe.brushfire

import spire.algebra.Order

// Backwards compat.

object SoftVoter {
  def apply[L, M: Numeric]() = Voter.soft[L, M]
}

object ModeVoter {
  def apply[L, M: Order]() = Voter.mode[L, M]
}

object ThresholdVoter {
  import Voter.FrequencyVoter

  def apply[M](threshold: Double, voter: FrequencyVoter[Boolean, M]) =
    Voter.threshold(threshold, voter)
}
