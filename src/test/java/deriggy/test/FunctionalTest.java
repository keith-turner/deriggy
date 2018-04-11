package deriggy.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import deriggy.api.Deriggy;
import deriggy.api.Edge;
import deriggy.api.Id;
import deriggy.api.Neighbor;
import deriggy.api.NodeObserver;
import deriggy.api.NodeObserver.AliasEdge;
import deriggy.api.NodeObserver.Direction;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.api.observer.ObserverProvider;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FunctionalTest {

  // TODO test attributes

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final Column TEST_EDGE_COL = new Column("test", "edge");

  private static String encode(Id node) {
    if (node.isAlias()) {
      return "A" + node.nodeId;
    } else {
      return node.graphId + node.nodeId;
    }
  }

  private static String encode(Id node, AliasEdge ae) {
    String node1 = encode(node);
    String node2 = encode(ae.otherNode);
    String direction = ae.direction == Direction.OUT ? "->" : "<-";
    return node1 + direction + node2 + ":" + ae.type;
  }

  public static class TestNodeObserver implements NodeObserver {
    @Override
    public Map<String, String> process(TransactionBase tx, NodeState node) {
      for (AliasEdge aliasEdge : node.getAliasEdges()) {
        switch (aliasEdge.change) {
          case ADDED:
            tx.set("test:" + encode(node.getNodeId(), aliasEdge), TEST_EDGE_COL, "");
            break;
          case DELETED:
            tx.delete("test:" + encode(node.getNodeId(), aliasEdge), TEST_EDGE_COL);
            break;
          case NONE:
            break;
          default:
            throw new IllegalArgumentException();
        }
      }

      return Collections.emptyMap();
    }
  }

  public static class TestOP implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      Deriggy.registerObservers(or, new TestNodeObserver());
    }
  }

  private static Set<String> getTestRows(FluoClient client) {
    TreeSet<String> rows = new TreeSet<>();

    try (Snapshot snap = client.newSnapshot()) {
      snap.scanner().overPrefix("test:").build()
          .forEach(rcv -> rows.add(rcv.getsRow().substring(5)));
    }
    return rows;
  }

  Set<String> genExpected(String... edges) {
    HashSet<String> expected = new HashSet<>();
    for (String edge : edges) {
      String[] nodes = edge.split(",");
      expected.add(nodes[0] + "->" + nodes[1] + ":f");
      expected.add(nodes[1] + "<-" + nodes[0] + ":f");
    }

    return expected;
  }

  public void checkEdges(FluoClient client, String... edges) {
    Set<String> actual = getTestRows(client);
    Set<String> expected = genExpected(edges);

    Assert.assertEquals("Diff : " + Sets.symmetricDifference(actual, expected), expected, actual);
  }

  @Test
  public void test1() throws Exception {
    FluoConfiguration conf = new FluoConfiguration();
    conf.setObserverProvider(TestOP.class);
    conf.setApplicationName("deriggy");
    conf.setMiniDataDir(folder.newFolder("miniFluo-1").toString());

    try (MiniFluo mini = FluoFactory.newMiniFluo(conf);
        FluoClient client = FluoFactory.newClient(mini.getClientConfiguration())) {

      Deriggy deriggy = new Deriggy();

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        le.execute((tx, ctx) -> deriggy.addEdge(tx, new Edge("T", "1", "3", "f")));
        le.execute((tx, ctx) -> deriggy.addEdge(tx, new Edge("T", "2", "4", "f")));
        le.execute((tx, ctx) -> deriggy.addEdge(tx, new Edge("G", "1", "2", "f")));
        le.execute((tx, ctx) -> deriggy.addEdge(tx, new Edge("G", "4", "1", "f")));
        le.execute((tx, ctx) -> deriggy.reconcileEdges(tx, new Id("F", "3"), ImmutableSet
            .of(new Neighbor("1", "f"), new Neighbor("2", "f"), new Neighbor("5", "f"))));
      }


      mini.waitForObservers();
      checkEdges(client, "T1,T3", "T2,T4", "G1,G2", "G4,G1", "F3,F1", "F3,F2", "F3,F5");

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        for (String graphId : Arrays.asList("T", "F", "G")) {
          for (String nodeID : Arrays.asList("1", "2", "3", "4", "5")) {
            le.execute((tx, ctx) -> deriggy.setAlias(tx, new Id(graphId, nodeID), "0" + nodeID));
          }
        }
      }

      mini.waitForObservers();
      checkEdges(client, "A01,A02", "A01,A03", "A03,A01", "A03,A02", "A03,A05", "A04,A01",
          "A02,A04");

      try (Transaction tx = client.newTransaction()) {
        deriggy.reconcileEdges(tx, new Id("T", "2"),
            ImmutableSet.of(new Neighbor("1", "f"), new Neighbor("3", "f")));
        tx.commit();
      }

      mini.waitForObservers();
      checkEdges(client, "A01,A02", "A01,A03", "A03,A01", "A03,A02", "A03,A05", "A04,A01",
          "A02,A03", "A02,A01");
    }
  }
}
