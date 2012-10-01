package com.fasterxml.storemate.shared.key;

import java.util.BitSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Logical slice of a {@link KeySpace}; a consecutive range of individual
 * key values within value space, defined by two parameters:
 * starting key value (inclusive), and length between 0 and length of
 * {@link KeySpace}.
 *<p>
 * Instances have natural ordering based on {@link #getStart()}, ascending.
 */
public class KeyRange implements Comparable<KeyRange>
{
    // [0,+60]
    protected final static Pattern DESC_PATTERN =
            Pattern.compile("\\[(\\d+),\\s*\\+(\\d+)\\]");
    
    /**
     * Key space this range is contained in.
     */
    protected final KeySpace _keyspace;

    /**
     * Local copy of the total length of space (to avoid lookups
     * from parent); used for figuring out where wrapping
     * should occur
     */
    protected final int _spaceEnd;
	
    protected final int _start;
	
    protected final int _length;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, factory methods
    ///////////////////////////////////////////////////////////////////////
     */

    protected KeyRange(//@JsonProperty("keyspace")
        KeySpace space,
        //@JsonProperty("start")
        int start,
        //@JsonProperty("length")
        int length)
    {
        _keyspace = space;
        _spaceEnd = space.getLength();
        _start = start;
        _length = length;
    }

    // Constructor used by deserializer
    protected KeyRange(External ext)
    {
        _keyspace = ext.keyspace;
        _spaceEnd = _keyspace.getLength();
        _start = ext.start;
        _length = ext.length;
    }
    
    public static KeyRange valueOf(KeySpace space, String ref)
            throws IllegalArgumentException
    {
        // let's accept null, empty String as empty range
        if (ref == null) {
            return space.emptyRange();
        }
        ref = ref.trim();
        if (ref.isEmpty()) {
            return space.emptyRange();
        }
        // otherwise must parse
        try {
            Matcher m = DESC_PATTERN.matcher(ref);
            if (m.matches()) {
                int start = Integer.parseInt(m.group(1));
                int len = Integer.parseInt(m.group(2));
                return new KeyRange(space, start, len);
            }
        } catch (IllegalArgumentException e) { }
        throw new IllegalArgumentException("Invalid KeyRange reference '"+ref+"'");
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Fluent factories (mutant factories)
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Fluent factory method for creating a range with same start
     * point, but possibly different length
     */
    public KeyRange withLength(int newLength)
            throws IllegalArgumentException
    {
        if (newLength == _length) {
            return this;
        }
        if (newLength < 0 || newLength > _spaceEnd) {
            throw new IllegalArgumentException("Illegal 'newLength' ("+newLength+"): must be [0, "
                    +_spaceEnd+"]");
        }
        return new KeyRange(_keyspace, _start, newLength);
    }

    /**
     * Method for calculating overlap range between this range and
     * given range; that is, range that is covered by both ranges.
     * If there is no overlap, an empty Range (with
     * start of 0, length of 0) will be returned.
     */
    public KeyRange intersection(KeyRange other) {
        return intersection(this, other);
    }
    
    /**
     * Method for calculating overlap between given ranges; that is, a range
     * that is covered by both ranges. If there is no overlap, an empty Range (with
     * start of 0, length of 0) will be returned.
     */
    public static KeyRange intersection(KeyRange range1, KeyRange range2)
    {
        /* First: one special case, which is rather difficult to handle
         * using general method -- that of "full ranges". Easiest thing
         * to do is to simply return "the other range" if either one is
         * full.
         */
        if (range1._coversWholeKeyspace()) {
            return range2;
        }
        if (range2._coversWholeKeyspace()) {
            return range1;
        }

        // Ok, let's see if range2 starts within range1:
        if (range1._containsPoint(range2.getStart())) {
            return _intersect(range1, range2);
        }
        if (range2._containsPoint(range1.getStart())) {
            return _intersect(range2, range1);
        }
        return range1._keyspace.emptyRange();
    }
    
    // Helper method called when we know range2 starst within range1
    private final static KeyRange _intersect(KeyRange range1, KeyRange range2)
    {
        // potential length of range1 needs to be subtracted by 'leading' part of range2
        int len = range1.getLength() - range1._clockwiseDistance(range2.getStart());
        len = Math.min(len, range2.getLength());
        return range2.withLength(len);
    }

    /**
     * Method for calculating combined range formed by this range
     * and given range; ranges must overlap or be adjacent (if they
     * are not, {@link IllegalArgumentException} will be thrown)
     */
    public KeyRange union(KeyRange other) {
        return union(this, other);
    }
    
    /**
     * Method for calculating combined range formed by two specified
     * overlapping or adjacent ranges
     * Ranges must overlap or be adjacent: if they
     * don't, {@link IllegalArgumentException} will be thrown;
     * that is, this method does not work for disjoint ranges.
     */
    public static KeyRange union(KeyRange range1, KeyRange range2)
    {
        // First two special cases we can weed out
        if (range1._coversWholeKeyspace()) {
            return range1;
        }
        if (range2._coversWholeKeyspace()) {
            return range1;
        }
        
        final int start1 = range1.getStart();
        final int start2 = range2.getStart();

        // Ok, let's see if range2 starts within or next to range1:
        if (range1._containsOrBordersPoint(start2)) {
            // if so, see how far it extends
            int newLen = range1._clockwiseDistance(start2) + range2.getLength();
            if (newLen < range1.getLength()) {
                return range1;
            }
            // note: can't exceed max length (could if union "overlaps with itself")
            return range1.withLength(Math.min(newLen, range1._spaceEnd));
        }
        if (range2._containsOrBordersPoint(start1)) {
            int newLen = range2._clockwiseDistance(start1) + range1.getLength();
            if (newLen < range2.getLength()) {
                return range2;
            }
            return range2.withLength(Math.min(newLen, range2._spaceEnd));
        }
        throw new IllegalArgumentException("Ranges "+range1+" and "+range2+" do not overlap or be adjacent");
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Simple accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public KeySpace getKeyspace() { return _keyspace; }

    public int getStart() { return _start; }
    public int getLength() { return _length; }

    public void fill(BitSet bits)
    {
        if (wrapped()) {
            bits.set(_start, _spaceEnd);
            bits.set(0, (_start+_length) - _spaceEnd);
        } else {
            bits.set(_start, _start + _length);
        }
    }
    
    /**
     * Method used for checking whether given hash key is contained
     * in this range
     */
    public boolean contains(KeyHash key) {
        return _containsPoint(key.getModuloHash());
    }

    /**
     * Alter lookup method used in cases where caller wants to minimize
     * allocations of {@link KeyHash} instances.
     * NOTE: rarely needed; only used by certain bulk operations.
     */
    public boolean contains(int rawHash) {
        int mod = KeyHash.calcModulo(rawHash, _spaceEnd);
        return _containsPoint(mod);
    }
    
    /**
     * Method used for checking whether given key range is fully contained
     * in this range or not.
     */
    public boolean contains(KeyRange other)
    {
        // one way to solve is to first handle non-wrapped-around case:
        int thisEnd = _linearEnd();
        int thatEnd = other._linearEnd();

        if ((other._start >= _start) && (thatEnd <= thisEnd)) {
            return true;
        }

        // then: wrap-around case, bit more complex
        if (thisEnd >= _spaceEnd) {
            // either way, can not contain something that's bigger than this one
            if (other._length > _length) {
                return false;
            }
            // offset space to the "other end" then
            thisEnd -= _spaceEnd;
            int thisStart = thisEnd - _length;
            if ((other._start >= thisStart) && (thatEnd <= thisEnd)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Method for checking whether given range overlaps with this
     * range.
     */
    public boolean overlapsWith(KeyRange other) {
        return rangesOverlap(this, other);
    }

    /**
     * Method for checking whether given ranges overlap
     */
    public static boolean rangesOverlap(KeyRange range1, KeyRange range2) {
        return range1._containsPoint(range2.getStart()) || range2._containsPoint(range1.getStart());
    }
	
    /**
     * Method that calculates clock-wise distance given key has, from
     * beginning of this range. This is typically used to find the
     * "closest" range for given key.
     *<br>
     * NOTE: method does NOT check whether this range actually contains
     * key (i.e. that {@link #contains} would return true); it merely calculates
     * distance from the start assuming range was large enough.
     * If you want to verify containment, you may want to instead call
     * {@link #clockwiseDistanceIfContained(KeyHash, int)}.
     */
    public int clockwiseDistance(KeyHash key)
    {
        return _clockwiseDistance(key.getModuloHash());
    }

    public int clockwiseDistanceIfContained(KeyHash key, int notContainedMarker)
    {
        int dist = _clockwiseDistance(key.getModuloHash());
        if (dist < _length) { // yes, is contained
            return dist;
        }
        // nope: return marker
        return notContainedMarker;
    }

    /**
     * Convenience method that is equivalent of:
     *<pre>
     *   getLength() == 0
     *</pre>
     */
    public boolean empty() { return _length == 0; }

    /**
     * Method that can be used to check whether this range "wraps"
     * around end of linear range.
     * 
     * @return True if range extends past logical end and wraps so that
     *   it covers both "end" and "start" of linear key space
     */
    public boolean wrapped() { return (_start + _length) > _spaceEnd; }
	
    /*
    ///////////////////////////////////////////////////////////////////////
    // Overrides
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public boolean equals(Object o)
    {
        if (o == this) return true;
        if (o == null) return false;
        if (o.getClass() != getClass()) return false;
        KeyRange other = (KeyRange) o;
        // should we compare key space directly? No, should be enough to verify space length:
        return (other._start == _start)
                && (other._length == _length)
                && (other._spaceEnd == _spaceEnd);
    }

    @Override
    public int hashCode() { return _length; }
    
    @Override
    public String toString() {
        return "["+_start+",+"+_length+"]";
    }

    @Override
    public int compareTo(KeyRange other) {
        int otherStart = other._start;
        if (_start < otherStart) return -1;
        return (_start == otherStart) ? 0 : 1;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Internal methods
    ///////////////////////////////////////////////////////////////////////
     */

    protected int _clockwiseDistance(int point)
    {
        int dist = point - _start;
        if (dist < 0) { // wrap-around case
            dist += _spaceEnd;
        }
        return dist;
    }
    
    protected boolean _containsPoint(int point)
    {
        int linearEnd =  _start + _length;
        if (point >= _start) {
            return (point < linearEnd);
        }
        // if range is wrapped, may still contain in wrapped beginning
        int wrappedEnd = linearEnd - _spaceEnd;
        return (point < wrappedEnd);
    }

    protected boolean _containsOrBordersPoint(int point)
    {
        int linearEnd =  _start + _length;
        if (point >= _start) {
            return (point <= linearEnd);
        }
        // if range is wrapped, may still contain in wrapped beginning
        int wrappedEnd = linearEnd - _spaceEnd;
        return (point <= wrappedEnd);
    }
    
    protected int _linearEnd() {
        return _start + _length;
    }

    protected boolean _coversWholeKeyspace() {
        return _length == _spaceEnd;
    }

    /*
    /**********************************************************************
    /* Helper types for (de)serialization
    /**********************************************************************
     */
    
    /**
     * Helper class used for deserialization, used as "delegating"
     * class
     */
    public static class External
    {
        public KeySpace keyspace;
        public int start;
        public int length;
    }
}
