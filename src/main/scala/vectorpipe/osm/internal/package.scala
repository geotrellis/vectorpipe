package vectorpipe.osm

import org.apache.spark.rdd._


package object internal {

  /** Given one RDD of nodes and one of ways, produce an RDD of all nodes/ways keyed by the WayID
   *  to which they are all related
   */
  def waySnapshots(
    nodes: RDD[(Long, Node)],
    ways: RDD[(Long, Way)]
  ): RDD[(Long, (Iterable[Way], Iterable[Node]))] = {

    val nodesToWayIds: RDD[(Node, Iterable[Long])] =
      nodes
        .cogroup(
          ways
            .flatMap { case (wayId, way) =>
              way.nodes.map { nodeId => (nodeId, wayId) }
            }
        )
        .flatMap { case (_, (nodes, wayIds)) =>
          if(nodes.isEmpty) { None }
          else {
            /* ASSUMPTION: `nodes` contains distinct elements */
            val node = nodes.head
            Some((node, wayIds))
          }
        }


    val wayIdToNodes: RDD[(Long, Node)] =
      nodesToWayIds
        .flatMap { case (node, wayIds) =>
          wayIds.map(wayId => (wayId, node))
        }

    ways.cogroup(wayIdToNodes)
  }
}
