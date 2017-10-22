import static org.junit.Assert.*;

import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class LinHashMapTest {

    LinHashMap<Integer, Integer> lhm;

    /**
     *
     * Sets up the linear hash map for testing purposes
     *
     **/
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    public void setUp() {
	lhm = new LinHashMap(Integer.class, Integer.class, 10);
    }

    /**
     *
     * Inputs key/value pairs into the linear hash map, and
     * then gets each key to see if the value equals the intended value.
     *
     **/

    @Test
    public void testPutGet() {

	lhm.put(8,  10);
	lhm.put(19,  4);
	lhm.put(12,  2);
	lhm.put(5,  17);
	lhm.put(6,  11);
	lhm.put(11,  14);
	lhm.put(7, 22);
	lhm.put(1,  6);
	lhm.put(21, 16);
	lhm.put(25,  3);
	
	assertEquals((int)lhm.get(8), 10);
	assertEquals((int)lhm.get(19), 4);
	assertEquals((int)lhm.get(12), 2);
	assertEquals((int)lhm.get(5), 17);
	assertEquals((int)lhm.get(6), 11);
	assertEquals((int)lhm.get(11), 14);
	assertEquals((int)lhm.get(7), 22);
	assertEquals((int)lhm.get(1), 6);
	assertEquals((int)lhm.get(21), 16);
	assertEquals((int)lhm.get(25), 3);
    }
    

    /**
     *
     * Tests the entrySet() method to see if it properly stores
     * the key/value pairs in the linear hash map.
     *
     */

    @Test
    public void testEntrySet() {
	
	testPutGet();
	
	Set<Entry<Integer, Integer>> test = lhm.entrySet();
	
	assertEquals(test.size(), 10);
    }
}
