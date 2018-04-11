package deriggy.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.gson.Gson;
import deriggy.api.NodeObserver.AliasEdge;
import deriggy.api.NodeObserver.Change;
import deriggy.api.NodeObserver.Direction;
import deriggy.api.NodeObserver.NodeState;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.observer.StringObserver;

class AliasNodeObserverImpl implements StringObserver {

  static final Column EDGE_STATUS_COL = new Column("edge", "stat");

  private final NodeObserver nodeObserver;

  public AliasNodeObserverImpl(NodeObserver nodeObserver) {
    this.nodeObserver = Objects.requireNonNull(nodeObserver);
  }

  private Id parseId(String alias) {
    if (alias.startsWith("a-")) {
      return new Id(Id.ALIAS_GRAPH_ID, alias.substring(2));
    } else {
      String[] fields = alias.split("-");
      return new Id(fields[1], fields[2]);
    }
  }

  @Override
  public void process(TransactionBase tx, String ntfyRow, Column col) throws Exception {
    RowScanner scanner = tx.scanner().overPrefix(ntfyRow).byRow().build();

    List<AliasEdge> edges = new ArrayList<>();
    Map<String, Map<Id, String>> attrs = new HashMap<>();

    Id myId = parseId(ntfyRow.split(":")[1]);

    for (ColumnScanner columns : scanner) {
      String currRow = columns.getsRow();
      if (currRow.equals(ntfyRow)) {
        // node info
        for (ColumnValue cv : columns) {
          if (cv.getColumn().getsFamily().equals(Deriggy.ATTR_FAM)) {
            @SuppressWarnings("unchecked")
            Map<String, String> rawAttrs = new Gson().fromJson(cv.getsValue(), Map.class);
            String[] tuple = cv.getColumn().getsQualifier().split(":");
            Id rawId = new Id(tuple[0], tuple[1]);
            rawAttrs.forEach((k, v) -> {
              attrs.computeIfAbsent(k, k2 -> new HashMap<>()).put(rawId, v);
            });
          }
        }
      } else {
        String fields[] = currRow.split(":");
        String direction = fields[2];
        String otherAlias = fields[3];
        String type = fields[4];

        String status = null;
        List<Edge> sourceEdges = new ArrayList<>();

        for (ColumnValue cv : columns) {
          if (cv.getColumn().equals(EDGE_STATUS_COL)) {
            status = cv.getsValue();
          } else if (cv.getColumn().getsFamily().equals("source")) { // TODO constant
            sourceEdges.add(Deriggy.parseEdge(cv.getColumn().getsQualifier()));
          }
        }

        Change change;
        if (status == null && sourceEdges.size() > 0) {
          change = Change.ADDED;
          tx.set(currRow, EDGE_STATUS_COL, "processed");
        } else if (status != null && sourceEdges.size() == 0) {
          change = Change.DELETED;
          tx.delete(currRow, EDGE_STATUS_COL);
        } else {
          change = Change.NONE;
        }

        edges.add(new AliasEdge(parseId(otherAlias), type, change, Direction.valueOf(direction),
            sourceEdges));
      }
    }

    nodeObserver.process(tx, new NodeState() {

      @Override
      public Id getNodeId() {
        return myId;
      }

      @Override
      public Iterable<AliasEdge> getAliasEdges() {
        return edges;
      }

      @Override
      public Map<String, Map<Id, String>> getPreviousAttributes() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Map<String, Map<Id, String>> getAttributes() {
        return attrs;
      }
    });
  }

}
