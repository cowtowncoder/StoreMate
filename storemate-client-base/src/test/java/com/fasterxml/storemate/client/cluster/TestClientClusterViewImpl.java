package com.fasterxml.storemate.client.cluster;

import com.fasterxml.storemate.api.KeyRange;
import com.fasterxml.storemate.api.KeySpace;
import com.fasterxml.storemate.client.cluster.ClusterViewByClientImpl;
import com.fasterxml.storemate.client.cluster.NodesForKey;
import com.fasterxml.storemate.client.cluster.ServerNodeStateImpl;

public class TestClientClusterViewImpl extends ClientTestBase
{
    protected final KeySpace DEFAULT_SPACE = new KeySpace(360);

    protected final KeyRange range1 = DEFAULT_SPACE.range(0, 120); // 0 - 119
    protected final KeyRange range2 = DEFAULT_SPACE.range(90, 60); // 90 - 149
    protected final KeyRange range3 = DEFAULT_SPACE.range(300, 90); // 300 - 29

    protected final ServerNodeStateImpl node1 = ServerNodeStateImpl.forTesting(range1);
    protected final ServerNodeStateImpl node2 = ServerNodeStateImpl.forTesting(range2);
    protected final ServerNodeStateImpl node3 = ServerNodeStateImpl.forTesting(range3);

    protected final ServerNodeStateImpl[] allNodes = new ServerNodeStateImpl[] { node1, node2, node3 };
    
    public void testSimpleDistanceCalc()
    {
        ClusterViewByClientImpl view = ClusterViewByClientImpl.forTesting(DEFAULT_SPACE);
        NodesForKey nodes;

        // first: test with all enabled
        
        // 2 ranges overlap; range2 closer to start
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(100), allNodes);
        assertEquals(2, nodes.size());
        assertEquals(90, nodes.node(0).getTotalRange().getStart());
        assertEquals(0, nodes.node(1).getTotalRange().getStart());

        // 2 here too
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(10), allNodes);
        assertEquals(2, nodes.size());
        assertEquals(0, nodes.node(0).getTotalRange().getStart());
        assertEquals(300, nodes.node(1).getTotalRange().getStart());
        
        // nothing covers this:
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(180), allNodes);
        assertEquals(0, nodes.size());

        // and just one node these:
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(80), allNodes);
        assertEquals(1, nodes.size());
        assertEquals(0, nodes.node(0).getTotalRange().getStart());

        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(350), allNodes);
        assertEquals(1, nodes.size());
        assertEquals(300, nodes.node(0).getTotalRange().getStart());
    }

    public void testDistanceCalcWithDisabled()
    {
        ClusterViewByClientImpl view = ClusterViewByClientImpl.forTesting(DEFAULT_SPACE);
        NodesForKey nodes;

        ServerNodeStateImpl disabledNode2 = ServerNodeStateImpl.forTesting(range2);
        disabledNode2.updateDisabled(true);
        
        ServerNodeStateImpl[] disabledStates = new ServerNodeStateImpl[] { node1, disabledNode2, node3 };
        
        // 2 ranges overlap; range2 would be closer but is disabled
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(100), disabledStates);
        assertEquals(2, nodes.size());
        assertEquals(0, nodes.node(0).getTotalRange().getStart());
        assertEquals(90, nodes.node(1).getTotalRange().getStart());

        // 2 here too; no change
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(10), disabledStates);
        assertEquals(2, nodes.size());
        assertEquals(0, nodes.node(0).getTotalRange().getStart());
        assertEquals(300, nodes.node(1).getTotalRange().getStart());
        
        // nothing covers this either
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(180), allNodes);
        assertEquals(0, nodes.size());
    }

    public void testActiveVsPassive()
    {
        ClusterViewByClientImpl view = ClusterViewByClientImpl.forTesting(DEFAULT_SPACE);

        KeyRange range1a = DEFAULT_SPACE.range(30, 60); // 30 - 89
        KeyRange range1p = DEFAULT_SPACE.range(0, 120); // 0 - 119

        KeyRange range2a = DEFAULT_SPACE.range(60, 60); // 60 - 119
        KeyRange range2p = DEFAULT_SPACE.range(30, 120); // 30 - 149

        ServerNodeStateImpl node1 = ServerNodeStateImpl.forTesting(range1a, range1p);
        ServerNodeStateImpl node2 = ServerNodeStateImpl.forTesting(range2a, range2p);

        NodesForKey nodes;

        // overlap; both included...
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(30),
                new ServerNodeStateImpl[] { node1, node2 });
        assertEquals(2, nodes.size());
        assertSame(node1, nodes.node(0));
        assertSame(node2, nodes.node(1));

        // changing order should not matter:
        nodes = view._calculateNodes(1, DEFAULT_SPACE.hash(30),
                new ServerNodeStateImpl[] { node2, node1 });
        assertEquals(2, nodes.size());
        assertSame(node1, nodes.node(0));
        assertSame(node2, nodes.node(1));
    }
    
    public void testFullCoverage()
    {
        ClusterViewByClientImpl view = ClusterViewByClientImpl.forTesting(DEFAULT_SPACE);
        assertEquals(210, view._getCoverage(allNodes));

        KeyRange extraRange = DEFAULT_SPACE.range(100, 200);
        ServerNodeStateImpl extraNode = ServerNodeStateImpl.forTesting(extraRange);

        ServerNodeStateImpl[] moreNodes = new ServerNodeStateImpl[] { node1, node2, node3, extraNode };
        assertEquals(360, view._getCoverage(moreNodes));
    }
}
