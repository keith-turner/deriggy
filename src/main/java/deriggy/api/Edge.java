package deriggy.api;

import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * An edge between nodes in the same graph.
 */
public class Edge {

  final String graphId;
  final String id1;
  final String id2;
  final String type;

  public Edge(String graph, String id1, String id2, String type) {
    this.graphId = Objects.requireNonNull(graph);
    this.id1 = Objects.requireNonNull(id1);
    this.id2 = Objects.requireNonNull(id2);
    this.type = Objects.requireNonNull(type);
    Preconditions.checkArgument(!id1.equals(id2));
  }

  @Override
  public int hashCode() {
    return Objects.hash(graphId, id1, id2, type);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Edge) {
      Edge oedge = (Edge) obj;
      return graphId.equals(oedge.graphId) && id1.equals(oedge.id1) && id2.equals(oedge.id2)
          && type.equals(oedge.type);
    }
    return false;
  }

  @Override
  public String toString() {
    return id1 + ":" + id2 + ":" + type;
  }
}
