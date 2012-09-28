package com.fasterxml.storemate.shared.key;

import com.fasterxml.storemate.shared.SharedTestBase;

public class RangeTest extends SharedTestBase
{
    protected final KeySpace DEFAULT_SPACE = new KeySpace(360);
	
    /*
    ///////////////////////////////////////////////////////////////////////
    // Basic range tests
    ///////////////////////////////////////////////////////////////////////
     */

    public void testToAndFromString()
    {
        KeyRange r = DEFAULT_SPACE.range(100, 50);
        String str = r.toString();
        assertEquals("[100,+50]", str);
        KeyRange r2 = DEFAULT_SPACE.range(str);
        assertEquals(100, r2.getStart());
        assertEquals(50, r2.getLength());
        assertEquals(r, r2);

        // also: empty String -> empty range
        r2 = DEFAULT_SPACE.range("");
        assertTrue(r2.empty());
        assertEquals(0, r2.getStart());
        assertEquals(0, r2.getLength());
    }
    
    public void testInvalidRanges()
    {
        // can't create range starting past end
        try {
            DEFAULT_SPACE.range(360, 10);
        } catch (IllegalArgumentException e) {
            verifyException(e, "Invalid 'from'");
        }
        // length can't exceed space length
        try {
            DEFAULT_SPACE.range(0, 361);
        } catch (IllegalArgumentException e) {
            verifyException(e, "Invalid 'length'");
        }
        // no negative start
        try {
            DEFAULT_SPACE.range(-1, 10);
        } catch (IllegalArgumentException e) {
            verifyException(e, "Invalid 'from'");
        }
        // no negative length
        try {
            DEFAULT_SPACE.range(10, -1);
        } catch (IllegalArgumentException e) {
            verifyException(e, "Invalid 'length'");
        }
    }

    public void testSimpleRange()
    {
        KeyRange range = DEFAULT_SPACE.range(100, 20);
        assertFalse(range.empty());
        assertFalse(range.wrapped());

        assertTrue(range.contains(DEFAULT_SPACE.hash(100)));
        assertTrue(range.contains(DEFAULT_SPACE.hash(119)));

        assertFalse(range.contains(DEFAULT_SPACE.hash(99)));
        assertFalse(range.contains(DEFAULT_SPACE.hash(120)));
        assertFalse(range.contains(DEFAULT_SPACE.hash(359)));
    }

    public void testEmptyRange()
    {
        KeyRange range = DEFAULT_SPACE.range(100, 0);
        assertTrue(range.empty());
        assertFalse(range.wrapped());
    }

    public void testWrappedRange()
    {
        KeyRange range = DEFAULT_SPACE.range(300, 100);
        assertFalse(range.empty());
        assertTrue(range.wrapped());

        assertTrue(range.contains(DEFAULT_SPACE.hash(300)));
        assertTrue(range.contains(DEFAULT_SPACE.hash(359)));
        assertTrue(range.contains(DEFAULT_SPACE.hash(0)));
        assertTrue(range.contains(DEFAULT_SPACE.hash(39)));

        assertFalse(range.contains(DEFAULT_SPACE.hash(40)));
        assertFalse(range.contains(DEFAULT_SPACE.hash(299)));

        // also, verify that "almost wrapped" isn't:
        assertFalse(DEFAULT_SPACE.range(300, 60).wrapped());
        assertFalse(DEFAULT_SPACE.range(0, 10).wrapped());
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Containment
    ///////////////////////////////////////////////////////////////////////
     */

    public void testRangeContains()
    {
        KeyRange mainRange = DEFAULT_SPACE.range(270, 180);

        // simpleish, non overlap
        assertTrue(mainRange.contains(DEFAULT_SPACE.range(270, 10)));
        assertTrue(mainRange.contains(DEFAULT_SPACE.range(270, 90)));
        assertTrue(mainRange.contains(DEFAULT_SPACE.range(270, 180)));
        assertTrue(mainRange.contains(DEFAULT_SPACE.range(300, 10)));

        // simpleish, wrapped
        assertTrue(mainRange.contains(DEFAULT_SPACE.range(350, 50)));
        // fully wrapped
        assertTrue(mainRange.contains(DEFAULT_SPACE.range(0, 90)));

        // then non-contained
        assertFalse(mainRange.contains(DEFAULT_SPACE.range(270, 181)));
        assertFalse(mainRange.contains(DEFAULT_SPACE.range(269, 180)));
        assertFalse(mainRange.contains(DEFAULT_SPACE.range(269, 181)));
        assertFalse(mainRange.contains(DEFAULT_SPACE.range(0, 360)));
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Distance calculations
    ///////////////////////////////////////////////////////////////////////
     */

    public void testDistances()
    {
        // half of the space:
        KeyRange range = DEFAULT_SPACE.range(90, 180);
        // first, contained
        assertEquals(0, range.clockwiseDistance(DEFAULT_SPACE.hash(90)));
        assertEquals(0, range.clockwiseDistanceIfContained(DEFAULT_SPACE.hash(90), -1));
        assertEquals(90, range.clockwiseDistance(DEFAULT_SPACE.hash(180)));
        assertEquals(90, range.clockwiseDistanceIfContained(DEFAULT_SPACE.hash(180), -1));
        assertEquals(179, range.clockwiseDistance(DEFAULT_SPACE.hash(269)));
        assertEquals(179, range.clockwiseDistanceIfContained(DEFAULT_SPACE.hash(269), -1));

        assertEquals(180, range.clockwiseDistance(DEFAULT_SPACE.hash(270)));
        assertEquals(-1, range.clockwiseDistanceIfContained(DEFAULT_SPACE.hash(270), -1));
        assertEquals(359, range.clockwiseDistance(DEFAULT_SPACE.hash(89)));
        assertEquals(-1, range.clockwiseDistanceIfContained(DEFAULT_SPACE.hash(89), -1));
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Overlaps
    ///////////////////////////////////////////////////////////////////////
     */

    public void testNoOverlap()
    {
        KeyRange r = DEFAULT_SPACE.range(100, 20).intersection(DEFAULT_SPACE.range(200, 30));
        assertTrue(r.empty());
        
        r = DEFAULT_SPACE.range(300, 60).intersection(DEFAULT_SPACE.range(0, 60));
        assertTrue(r.empty());
    }   

    public void testSimpleOverlap()
    {
        KeyRange range1 = DEFAULT_SPACE.range(90, 20);
        KeyRange range2 = DEFAULT_SPACE.range(100, 20);

        KeyRange overlap = range1.intersection(range2);
        assertEquals(100, overlap.getStart());
        assertEquals(10, overlap.getLength());
        
        // should work the other way around too
        overlap = range1.intersection(range2);
        assertEquals(100, overlap.getStart());
        assertEquals(10, overlap.getLength());
        
        overlap = KeyRange.intersection(range1, range2);
        assertEquals(100, overlap.getStart());
        assertEquals(10, overlap.getLength());

        range1 = DEFAULT_SPACE.range(0, 270);
        range2 = DEFAULT_SPACE.range(90, 300); // from 90 to 30
        overlap = KeyRange.intersection(range1, range2);
        assertEquals(90, overlap.getStart());
        assertEquals(180, overlap.getLength());

        // one more, based on observed anomaly
        range1 = DEFAULT_SPACE.range(0, 180);
        range2 = DEFAULT_SPACE.range(270, 180);
        overlap = KeyRange.intersection(range1, range2);
        assertEquals(0, overlap.getStart());
        assertEquals(90, overlap.getLength());
        // and vice versa:
        overlap = KeyRange.intersection(range2, range1);
        assertEquals(0, overlap.getStart());
        assertEquals(90, overlap.getLength());
    }

    public void testContainedOverlap()
    {
        KeyRange range1 = DEFAULT_SPACE.range(90, 180);
        KeyRange range2 = DEFAULT_SPACE.range(130, 50);

        KeyRange overlap = range1.intersection(range2);
        assertEquals(130, overlap.getStart());
        assertEquals(50, overlap.getLength());
        overlap = range2.intersection(range1);
        assertEquals(130, overlap.getStart());
        assertEquals(50, overlap.getLength());
    }

    public void testWrappedOverlap()
    {
        KeyRange range1 = DEFAULT_SPACE.range(300, 100);
        KeyRange range2 = DEFAULT_SPACE.range(330, 100);

        KeyRange overlap = range1.intersection(range2);
        assertEquals(330, overlap.getStart());
        assertEquals(70, overlap.getLength());
        
        range1 = DEFAULT_SPACE.range(90, 180);
        range2 = DEFAULT_SPACE.range(270, 180);
        overlap = range1.intersection(range2);
        assertTrue(overlap.empty());

        // one more thing: edge case, but happens with 2-node setups:
        range1 = DEFAULT_SPACE.range(90, 360);
        range2 = DEFAULT_SPACE.range(270, 360);
        overlap = range1.intersection(range2);
        assertEquals(360, overlap.getLength());
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Union
    ///////////////////////////////////////////////////////////////////////
     */

    public void testUnionDisjoint()
    {
        KeyRange range1 = DEFAULT_SPACE.range(10, 20);
        KeyRange range2 = DEFAULT_SPACE.range(50, 20);
        try {
            range1.union(range2);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            verifyException(e, "do not overlap");
        }
    }

    public void testUnionIdentity()
    {
        KeyRange range = DEFAULT_SPACE.range(30, 30);
        KeyRange union = range.union(range);
        assertEquals(range, union);

        range = DEFAULT_SPACE.range(0, 360);
        union = range.union(range);
        assertEquals(range, union);
    }

    public void testUnionOverlapping()
    {
        // first, no wrap
        KeyRange range1 = DEFAULT_SPACE.range(30, 30);
        KeyRange range2 = DEFAULT_SPACE.range(50, 20);

        KeyRange union = range1.union(range2);
        assertEquals(30, union.getStart());
        assertEquals(40, union.getLength());

        union = range2.union(range1);
        assertEquals(30, union.getStart());
        assertEquals(40, union.getLength());

        // then with wrapping
        range1 = DEFAULT_SPACE.range(350, 30);
        range2 = DEFAULT_SPACE.range(0, 45);
        union = range1.union(range2);
        assertEquals(350, union.getStart());
        assertEquals(55, union.getLength());
    }        

    public void testUnionContained()
    {
        // first, no wrap
        KeyRange range1 = DEFAULT_SPACE.range(90, 180);
        KeyRange range2 = DEFAULT_SPACE.range(180, 30);

        KeyRange union = range1.union(range2);
        assertEquals(90, union.getStart());
        assertEquals(180, union.getLength());

        // then with wrap
        range1 = DEFAULT_SPACE.range(180, 270);
        range2 = DEFAULT_SPACE.range(240, 130);

        union = range1.union(range2);
        assertEquals(180, union.getStart());
        assertEquals(270, union.getLength());
    }
    
    public void testUnionAdjacent()
    {
        // first, wrapped case
        KeyRange range1 = DEFAULT_SPACE.range(350, 30);
        KeyRange range2 = DEFAULT_SPACE.range(20, 30);

        KeyRange union = range1.union(range2);
        assertEquals(350, union.getStart());
        assertEquals(60, union.getLength());

        union = range2.union(range1);
        assertEquals(350, union.getStart());
        assertEquals(60, union.getLength());

        // then straight
        range1 = DEFAULT_SPACE.range(30, 30);
        range2 = DEFAULT_SPACE.range(60, 40);

        union = range1.union(range2);
        assertEquals(30, union.getStart());
        assertEquals(70, union.getLength());
    }

    public void testUnionOverride() // i.e. union "overlaps itself"
    {
        KeyRange range1 = DEFAULT_SPACE.range(0, 270);
        KeyRange range2 = DEFAULT_SPACE.range(180, 270);

        KeyRange union = range1.union(range2);
        // starting point is actually arbitrary; but should be one of inputs
        if (union.getStart() != 0 && union.getStart() != 180) {
            fail("Strange start: "+union.getStart());
        }
        assertEquals(360, union.getLength());
    }
}
