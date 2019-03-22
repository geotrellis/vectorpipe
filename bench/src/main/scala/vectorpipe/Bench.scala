package vectorpipe

import java.util.concurrent.TimeUnit

import geotrellis.vector.{Extent, Line, Point}
import org.openjdk.jmh.annotations._

// --- //

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
class LineBench {
  val extent = Extent(0, 0, 5, 5)

  var line: Line = _

  @Setup
  def setup: Unit = {
    line = Line(
      List.range(4, -100, -2).map(n => Point(n, 1)) ++ List(Point(-3,4), Point(-1,4), Point(2,4), Point(4,4))
    )
  }
}
