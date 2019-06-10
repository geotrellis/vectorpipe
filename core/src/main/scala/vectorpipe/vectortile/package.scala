package vectorpipe

import geotrellis.proj4._
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.vectortile._
import org.apache.spark.sql._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.locationtech.jts.{geom => jts}

import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.{classTag, ClassTag}
import scala.util.{Try, Success, Failure}

package object vectortile {
  sealed trait LayerMultiplicity { val name: String }
  case class SingleLayer(val name: String) extends LayerMultiplicity
  case class LayerNamesInColumn(val name: String) extends LayerMultiplicity

  @transient lazy val logger = org.apache.log4j.Logger.getRootLogger

  type VectorTileFeature[+G <: Geometry] = Feature[G, Map[String, Value]]

  def caseClassToVTFeature[T: ClassTag]: T => VectorTileFeature[Geometry] = {
    // val accessors = typeOf[T].members.collect {
    //   case m: MethodSymbol if m.isCaseAccessor => m
    // }.toList
    // val jtsGeomType = typeOf[jts.Geometry]
    // assert(accessors.exists(_.returnType <:< jtsGeomType), "Automatic case class conversion to Features requires a JTS Geometry type")

    val fields = classTag[T].runtimeClass.getDeclaredFields.filterNot(_.isSynthetic).toList
    val jtsGeomType = classOf[jts.Geometry]
    assert(fields.exists{ f => classOf[jts.Geometry].isAssignableFrom(f.getType) },
           "Automatic case class conversion to Features requires a JTS Geometry type")

    { t: T =>
      fields.foldLeft( (None: Option[Geometry], Map.empty[String, Value]) ) { (state, field) =>
        field.getType match {
          case ty if jtsGeomType.isAssignableFrom(ty) =>
            field.setAccessible(true)
            val g = Geometry(field.get(t).asInstanceOf[jts.Geometry])
            field.setAccessible(false)
            (Some(g), state._2)
          case ty if ty == classOf[String] =>
            field.setAccessible(true)
            val v = field.get(t).asInstanceOf[String]
            field.setAccessible(false)
            (state._1, state._2 + (field.getName -> VString(v)))
          case ty if ty == classOf[Int] =>
            field.setAccessible(true)
            val v = field.getInt(t)
            field.setAccessible(false)
            (state._1, state._2 + (field.getName -> VInt64(v.toLong)))
          case ty if ty == classOf[Long] =>
            field.setAccessible(true)
            val v = field.getLong(t)
            field.setAccessible(false)
            (state._1, state._2 + (field.getName -> VInt64(v)))
          case ty if ty == classOf[Float] =>
            field.setAccessible(true)
            val v = field.getFloat(t)
            field.setAccessible(false)
            (state._1, state._2 + (field.getName -> VFloat(v)))
          case ty if ty == classOf[Double] =>
            field.setAccessible(true)
            val v = field.getDouble(t)
            field.setAccessible(false)
            (state._1, state._2 + (field.getName -> VDouble(v)))
          case _ =>
            logger.warn(s"Dropped ${field.getName} of incompatible type ${field.getType} from vector tile feature")
            state
        }
      } match {
        case (None, m) => throw new IllegalStateException("JTS Geometry type not found")
        case (Some(g), attrs) => Feature(g, attrs)
      }
    }
  }

  @transient lazy val st_reprojectGeom = udf { (g: jts.Geometry, srcProj: String, destProj: String) =>
    val trans = Proj4Transform(CRS.fromString(srcProj), CRS.fromString(destProj))
    val gt = Geometry(g)
    gt.reproject(trans).jtsGeom
  }

  def keyTo(layout: LayoutDefinition) = udf { g: jts.Geometry =>
    layout.mapTransform.keysForGeometry(geotrellis.vector.Geometry(g)).toArray
  }

  def getSpatialKey(k: GenericRowWithSchema): SpatialKey = SpatialKey(k.getInt(0), k.getInt(1))

  def getSpatialKey(row: Row, field: String): SpatialKey = {
    val k = row.getAs[Row](field)
    SpatialKey(k.getInt(0), k.getInt(1))
  }

  // case class IdFeature[+G <: Geometry, +D](geom: Geometry, data: D, id: Int) extends Feature[G, D](geom, data) {
  //   override def mapGeom[T <: Geometry](f: G => T): IdFeature[T, D] =
  //     IdFeature(f(geom), data, id)

  //   override def mapData[T](f: D => T): IdFeature[G, T] =
  //     IdFeature(geom, f(data), id)
  // }

  def timedIntersect[G <: Geometry](geom: G, ex: Extent)(implicit ec: ExecutionContext) = {
    val future = Future { geom.intersection(ex) }
    Try(Await.result(future, 5000 milliseconds)) match {
      case Success(res) => res
      case Failure(_) =>
        logger.warn(s"Could not intersect $geom with $ex in 5000 milliseconds")
        NoResult
    }
  }

  case class VTContents(points: List[VectorTileFeature[Point]] = Nil,
                        multipoints: List[VectorTileFeature[MultiPoint]] = Nil,
                        lines: List[VectorTileFeature[Line]] = Nil,
                        multilines: List[VectorTileFeature[MultiLine]] = Nil,
                        polygons: List[VectorTileFeature[Polygon]] = Nil,
                        multipolygons: List[VectorTileFeature[MultiPolygon]] = Nil) {
    def +(other: VTContents) = VTContents(points ++ other.points,
                                          multipoints ++ other.multipoints,
                                          lines ++ other.lines,
                                          multilines ++ other.multilines,
                                          polygons ++ other.polygons,
                                          multipolygons ++ other.multipolygons)
    def +[G <: Geometry](other: VectorTileFeature[G]) = other.geom match {
      case p : Point        => copy(points=other.asInstanceOf[VectorTileFeature[Point]] :: points)
      case mp: MultiPoint   => copy(multipoints=other.asInstanceOf[VectorTileFeature[MultiPoint]] :: multipoints)
      case l : Line         => copy(lines=other.asInstanceOf[VectorTileFeature[Line]] :: lines)
      case ml: MultiLine    => copy(multilines=other.asInstanceOf[VectorTileFeature[MultiLine]] :: multilines)
      case p : Polygon      => copy(polygons=other.asInstanceOf[VectorTileFeature[Polygon]] :: polygons)
      case mp: MultiPolygon => copy(multipolygons=other.asInstanceOf[VectorTileFeature[MultiPolygon]] :: multipolygons)
    }
  }
  object VTContents {
    def empty() = VTContents()
  }

  def buildLayer[G <: Geometry](features: Iterable[VectorTileFeature[G]], layerName: String, ex: Extent, tileWidth: Int): Layer = {
    val contents = features.foldLeft(VTContents.empty){ (accum, feature) => accum + feature }
    val VTContents(pts, mpts, ls, mls, ps, mps) = contents
    StrictLayer(
      name=layerName,
      tileWidth=tileWidth,
      version=2,
      tileExtent=ex,
      points=pts,
      multiPoints=mpts,
      lines=ls,
      multiLines=mls,
      polygons=ps,
      multiPolygons=mps
    )
  }

  def buildSortedLayer[G <: Geometry](features: Iterable[VectorTileFeature[G]], layerName: String, ex: Extent, tileWidth: Int): Layer = {
    val contents = features.foldLeft(VTContents.empty){ (accum, feature) => accum + feature }
    val VTContents(pts, mpts, ls, mls, ps, mps) = contents
    StrictLayer(
      name=layerName,
      tileWidth=tileWidth,
      version=2,
      tileExtent=ex,
      points=pts,
      multiPoints=mpts,
      lines=ls,
      multiLines=mls,
      polygons=ps.sortWith(_.area > _.area),
      multiPolygons=mps.sortWith(_.area > _.area)
    )
  }

  def buildVectorTile[G <: Geometry](
    features: Iterable[VectorTileFeature[G]],
    layerName: String,
    ex: Extent,
    tileWidth: Int,
    sorted: Boolean
  ): VectorTile = {
    val layer =
      if (sorted)
        buildSortedLayer(features, layerName, ex, tileWidth)
      else
        buildLayer(features, layerName, ex, tileWidth)
    VectorTile(Map(layerName -> layer), ex)
  }

  def buildVectorTile[G <: Geometry](
    layerFeatures: Map[String, Iterable[VectorTileFeature[G]]],
    ex: Extent,
    tileWidth: Int,
    sorted: Boolean
  ): VectorTile = {
    VectorTile(layerFeatures.map{ case (layerName, features) => (layerName,
      if (sorted)
        buildSortedLayer(features, layerName, ex, tileWidth)
      else
        buildLayer(features, layerName, ex, tileWidth))
    }, ex)
  }

}
