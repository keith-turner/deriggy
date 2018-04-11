package deriggy.api;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.gson.Gson;
import deriggy.api.NodeObserver.Direction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.observer.ObserverProvider.Registry;

import static org.apache.fluo.api.observer.Observer.NotificationType.WEAK;

/**
 * Simple library for maintaining a derived graph built on Apache Fluo. This library supports the
 * following features.
 *
 * <UL>
 * <LI>Multiple raw graphs
 * <LI>Aliases that link nodes in raw graphs into a derived graph
 * <LI>Node attributes
 * <LI>Ability to keep derived graph up to date as raw graph change.
 * </UL>
 *
 * @since 1.0.0
 */

public class Deriggy {

  static final String ATTR_FAM = "attr";
  static final Column RAW_EDGE = new Column("edge", "raw");
  static final Column ALIAS_COL = new Column("node", "alias");
  static final Column EDGE_SOURCE_COL = new Column("edge", "source");
  static final Column ALIAS_NODE_NTFY_COL = new Column("check", "alias-node");

  static final String IN = ":" + Direction.IN.name() + ":";
  static final String OUT = ":" + Direction.OUT.name() + ":";

  /**
   * Call this method in a Fluo Observer Provider to register something to observer changes to the
   * derived graph.
   */
  public static void registerObservers(Registry registry, NodeObserver dno) {
    registry.forColumn(ALIAS_NODE_NTFY_COL, WEAK).useObserver(new AliasNodeObserverImpl(dno));
  }

  static String encodeEdge(Edge edge) {
    return edge.graphId + ":" + edge.id1 + OUT + edge.id2 + ":" + edge.type;
  }

  static Edge parseEdge(String encodedEdge) {
    String[] fields = encodedEdge.split(":");
    return new Edge(fields[0], fields[1], fields[3], fields[4]);
  }

  private String encodeAlias(String graphId, String nodeId, String alias) {
    if (alias == null) {
      return "r-" + graphId + "-" + nodeId;
    } else {
      return "a-" + alias;
    }
  }

  private final String GID_ERR_MSG = "The graph id '" + Id.ALIAS_GRAPH_ID + "' is reserved";

  private void checkGraphId(String graphId) {
    Preconditions.checkArgument(!graphId.equals(Id.ALIAS_GRAPH_ID), GID_ERR_MSG);
  }

  private void removeEdge(TransactionBase tx, String alias1, String alias2, Edge edge) {
    String encodedEdge = encodeEdge(edge);
    tx.delete("r:" + encodedEdge, RAW_EDGE);
    tx.delete("d:" + alias1 + OUT + alias2 + ":" + edge.type, new Column("source", encodedEdge));
    tx.delete("d:" + alias2 + IN + alias1 + ":" + edge.type, new Column("source", encodedEdge));
    tx.setWeakNotification("d:" + alias2, ALIAS_NODE_NTFY_COL);
  }

  private void addEdge(TransactionBase tx, String alias1, String alias2, Edge edge) {
    String encodedEdge = encodeEdge(edge);
    tx.set("r:" + encodedEdge, RAW_EDGE, "");
    tx.set("d:" + alias1 + OUT + alias2 + ":" + edge.type, new Column("source", encodedEdge), "");
    tx.set("d:" + alias2 + IN + alias1 + ":" + edge.type, new Column("source", encodedEdge), "");
    tx.setWeakNotification("d:" + alias2, ALIAS_NODE_NTFY_COL);
  }

  /**
   * For the given node make its set of persistent edges consistent with the set of passed in
   * Neighbors. Removes existing edges to neighbors that are not in the passed in set. Adds
   * non-existant edges to neighbors in the passed in set.
   */
  public void reconcileEdges(TransactionBase tx, Id id, Set<Neighbor> neighbors) {
    checkGraphId(id.graphId);

    // get the current neighbors and determine if there is a difference with the passed in set
    CellScanner scanner = tx.scanner().overPrefix("r:" + id.graphId + ":" + id.nodeId)
        .fetch(RAW_EDGE).build();

    Set<Neighbor> currNeighbors = new HashSet<>();

    for (RowColumnValue rcv : scanner) {
      String[] fields = rcv.getsRow().split(":");
      currNeighbors.add(new Neighbor(fields[4], fields[5]));
    }

    SetView<Neighbor> symmetricDiff = Sets.symmetricDifference(neighbors, currNeighbors);

    if (symmetricDiff.size() > 0) {
      Map<String,RowColumn> aliasKeys = new HashMap<>();

      // create keys for looking up aliases for neighbors and self
      RowColumn myAliasKey = new RowColumn("r:" + id.graphId + ":" + id.nodeId, ALIAS_COL);
      aliasKeys.put(id.nodeId, new RowColumn("r:" + id.graphId + ":" + id.nodeId, ALIAS_COL));
      symmetricDiff.forEach(
          n -> aliasKeys.put(n.id, new RowColumn("r:" + id.graphId + ":" + n.id, ALIAS_COL)));

      // lookup current aliases using a read lock
      Map<RowColumn,String> aliases = tx.withReadLock().gets(aliasKeys.values());

      String myAlias = encodeAlias(id.graphId, id.nodeId, aliases.get(myAliasKey));

      // neighbors to add
      for (Neighbor neighbor : Sets.difference(neighbors, currNeighbors)) {
        String otherAlias = encodeAlias(id.graphId, neighbor.id,
            aliases.get(aliasKeys.get(neighbor.id)));
        addEdge(tx, myAlias, otherAlias,
            new Edge(id.graphId, id.nodeId, neighbor.id, neighbor.type));
      }

      // neighbors to remove
      for (Neighbor neighbor : Sets.difference(currNeighbors, neighbors)) {
        String otherAlias = encodeAlias(id.graphId, neighbor.id,
            aliases.get(aliasKeys.get(neighbor.id)));
        removeEdge(tx, myAlias, otherAlias,
            new Edge(id.graphId, id.nodeId, neighbor.id, neighbor.type));
      }

      tx.setWeakNotification("d:" + myAlias, ALIAS_NODE_NTFY_COL);
    }
  }

  /**
   * Add an edge between two nodes in a raw graph.
   */
  public void addEdge(TransactionBase tx, Edge edge) {
    checkGraphId(edge.graphId);

    String encodedEdge = encodeEdge(edge);

    // check if edge exists
    if (tx.gets("r:" + encodedEdge, RAW_EDGE) == null) {
      RowColumn aliasKey1 = new RowColumn("r:" + edge.graphId + ":" + edge.id1, ALIAS_COL);
      RowColumn aliasKey2 = new RowColumn("r:" + edge.graphId + ":" + edge.id2, ALIAS_COL);

      Map<RowColumn,String> aliases = tx.withReadLock().gets(Arrays.asList(aliasKey1, aliasKey2));

      String alias1 = encodeAlias(edge.graphId, edge.id1, aliases.get(aliasKey1));
      String alias2 = encodeAlias(edge.graphId, edge.id2, aliases.get(aliasKey2));

      addEdge(tx, alias1, alias2, edge);

      tx.setWeakNotification("d:" + alias1, ALIAS_NODE_NTFY_COL);

    }
  }

  /**
   * Remove an edge between two nodes in a raw graph.
   */
  public void removeEdge(TransactionBase tx, Edge edge) {
    checkGraphId(edge.graphId);

    String encodedEdge = encodeEdge(edge);
    if (tx.gets("r:" + encodedEdge, RAW_EDGE) != null) {
      RowColumn aliasKey1 = new RowColumn("r:" + edge.graphId + ":" + edge.id1, ALIAS_COL);
      RowColumn aliasKey2 = new RowColumn("r:" + edge.graphId + ":" + edge.id2, ALIAS_COL);

      Map<RowColumn,String> aliases = tx.withReadLock().gets(Arrays.asList(aliasKey1, aliasKey2));

      String alias1 = encodeAlias(edge.graphId, edge.id1, aliases.get(aliasKey1));
      String alias2 = encodeAlias(edge.graphId, edge.id2, aliases.get(aliasKey2));

      removeEdge(tx, alias1, alias2, edge);

      tx.setWeakNotification("d:" + alias1, ALIAS_NODE_NTFY_COL);
    }
  }

  private void _setAlias(TransactionBase tx, Id node, String alias) {
    checkGraphId(node.graphId);

    String nodeRow = "r:" + node.graphId + ":" + node.nodeId;
    String currentAlias = encodeAlias(node.graphId, node.nodeId, tx.gets(nodeRow, ALIAS_COL));
    String newAlias = encodeAlias(node.graphId, node.nodeId, alias);

    if (!newAlias.equals(currentAlias)) {
      CellScanner scanner = tx.scanner().overPrefix("d:" + currentAlias).fetch(new Column("source"))
          .build();

      if (alias == null) {
        tx.delete(nodeRow, ALIAS_COL);
      } else {
        tx.set(nodeRow, ALIAS_COL, alias);
      }

      for (RowColumnValue rcv : scanner) {
        String[] fields = rcv.getsRow().split(":");
        String dir = fields[2];
        String otherAlias = fields[3];
        String type = fields[4];

        // TODO constant

        // opposite direction
        String odir = dir.equals("OUT") ? ":IN:" : ":OUT:";
        dir = ":" + dir + ":";

        tx.delete(rcv.getRow(), rcv.getColumn());
        tx.delete("d:" + otherAlias + odir + currentAlias + ":" + type, rcv.getColumn());

        tx.set("d:" + otherAlias + odir + newAlias + ":" + type, rcv.getColumn(), "");
        tx.set("d:" + newAlias + dir + otherAlias + ":" + type, rcv.getColumn(), "");

        tx.setWeakNotification("d:" + otherAlias, ALIAS_NODE_NTFY_COL);
      }

      Column attrCol = new Column(ATTR_FAM, node.graphId + ":" + node.nodeId);
      String jsonAttr = tx.gets("d:" + currentAlias, attrCol);
      if (jsonAttr != null) {
        tx.delete("d:" + currentAlias, attrCol);
        tx.set("d:" + newAlias, attrCol, jsonAttr);
      }

      tx.setWeakNotification("d:" + currentAlias, ALIAS_NODE_NTFY_COL);
      tx.setWeakNotification("d:" + newAlias, ALIAS_NODE_NTFY_COL);
    }
  }

  /**
   * Link a node in a raw graph into the derived graph. This operation will map edges and attributes
   * associated with the raw id into the derived graph.
   *
   * @param node
   *          A raw graph id.
   * @param alias
   *          Derived graph id.
   */
  public void setAlias(TransactionBase tx, Id node, String alias) {
    _setAlias(tx, node, Objects.requireNonNull(alias));
  }

  /**
   * Unlink a node in a raw graph from any currently aliased id in the derived graph. This operation
   * will unmap edges and attributes associated with the raw id from the derived graph.
   *
   */
  public void removeAlias(TransactionBase tx, Id node) {
    _setAlias(tx, node, null);
  }

  /**
   * Set attributes on a node in a raw graph. These attributes will be mapped into the derived graph
   * using existing aliases.
   *
   */
  public void setAttributes(TransactionBase tx, Id node, Map<String,String> attributes) {
    checkGraphId(node.graphId);

    String nodeRow = "r:" + node.graphId + ":" + node.nodeId;

    String alias = encodeAlias(node.graphId, node.nodeId,
        tx.withReadLock().gets(nodeRow, ALIAS_COL));

    if (attributes.size() == 0) {
      tx.delete("d:" + alias, new Column(ATTR_FAM, node.graphId + ":" + node.nodeId));
    } else {
      Gson gson = new Gson();
      String jsonAttr = gson.toJson(attributes);
      // TODO could check if same as current
      tx.set("d:" + alias, new Column(ATTR_FAM, node.graphId + ":" + node.nodeId), jsonAttr);
    }

    tx.setWeakNotification("d:" + alias, ALIAS_NODE_NTFY_COL);
  }
}
