package deriggy.api;

import java.util.Collection;
import java.util.Map;

import org.apache.fluo.api.client.TransactionBase;

public interface NodeObserver {

  public static enum Change {
    ADDED, NONE, DELETED
  }

  public static enum Direction {
    IN, OUT
  }

  public static class AliasEdge {

    public final Id otherNode;
    public final String type;
    public final Change change;
    public final Direction direction;
    public final Collection<Edge> sourceEdges;

    public AliasEdge(Id otherNode, String type, Change change, Direction direction,
        Collection<Edge> sourceEdges) {
      super();
      this.otherNode = otherNode;
      this.type = type;
      this.change = change;
      this.direction = direction;
      this.sourceEdges = sourceEdges;
    }

  }

  public interface NodeState {
    Id getNodeId();

    Iterable<AliasEdge> getAliasEdges();

    Map<String, Map<Id, String>> getPreviousAttributes();

    Map<String, Map<Id, String>> getAttributes();
  }

  public Map<String, String> process(TransactionBase tx, NodeState node);
}
