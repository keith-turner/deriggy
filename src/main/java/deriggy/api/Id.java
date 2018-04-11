package deriggy.api;

import java.util.Objects;

public class Id {

  public static final Id EMPTY = new Id("", "");

  public static final String ALIAS_GRAPH_ID = "alias";

  public final String graphId;
  public final String nodeId;

  public Id(String graphId, String nodeId) {
    this.graphId = Objects.requireNonNull(graphId);
    this.nodeId = Objects.requireNonNull(nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(graphId, nodeId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Id) {
      Id oid = (Id) obj;
      return graphId.equals(oid.graphId) && nodeId.equals(oid.nodeId);
    }
    return false;
  }

  public boolean isAlias() {
    return graphId.equals(ALIAS_GRAPH_ID);
  }

  @Override
  public String toString() {
    return graphId + "/" + nodeId;
  }
}
