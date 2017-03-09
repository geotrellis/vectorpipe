package vectorpipe.geom

import geotrellis.vector._
import scaliper._

// --- //

class ClipBench extends Benchmarks with ConsoleReport {
  benchmark("toNearestPoint - short line") {
    run("Tail Recursion") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((-10 to 10).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPointR(extent, line)
      }
    }

    run("foldLeftM") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((-10 to 10).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPointF(extent, line)
      }
    }

    run("Java Style") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((-10 to 10).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPointJ(extent, line)
      }
    }
  }

  benchmark("toNearestPoint - medium line") {
    run("Tail Recursion") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((-100 to 100).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPointR(extent, line)
      }
    }

    run("foldLeftM") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((-100 to 100).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPointF(extent, line)
      }
    }

    run("Java Style") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((-100 to 100).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPointJ(extent, line)
      }
    }
  }
}
