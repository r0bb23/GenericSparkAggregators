package com.rbb.gsaggs

object MathHelpers {
    def harmonicMean(xs: Seq[Double]): Double = {
        val n = xs.length
        
        if (n > 1) {
            val invSum = xs.foldLeft(0.0) { (acc, x) => acc + 1.0 / x }
            n / invSum
        } else {
            xs(0)
        }
    }
}