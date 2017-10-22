import static java.lang.System.out;
import static org.junit.Assert.*;

import java.util.Set;
import java.util.SortedMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;

public class BpTreeMapTest {

    /**
     *
     * Creates new Bplus tree for testing purposes
     *
     * @return Bplus tree
     *
     */

    public BpTreeMap createTree(){
	
	BpTreeMap <Integer, Integer> bpt = new BpTreeMap <> (Integer.class, Integer.class);
        
	int keys = 10;
        
	bpt.put(8,  10);
	bpt.put(19,  4);
        bpt.put(12,  2);
        bpt.put(5,  17);
	bpt.put(6,  11);
        bpt.put(11,  14);
        bpt.put(7, 22);
	bpt.put(1,  6);
        bpt.put(21, 16);
	bpt.put(25,  3);

        return bpt;
    }

    /**
     *
     * Tests the firstKey method
     *
     */

    @Test
	
    public void testFirstKey() {
	
	BpTreeMap <Integer, Integer> bpt = createTree();
	
	assertEquals(1, (int)bpt.firstKey());
    }

    /**
     *
     * Tests the last key method
     *
     */

    @Test
	
    public void testLastKey() {
	
	BpTreeMap <Integer, Integer> bpt = createTree();
	
	assertEquals(25, (int)bpt.lastKey());
    }
    

    /**
     *
     * Tests the entrySet method 
     *
     */

    @Test
    
    public void testEntrySet() {

	BpTreeMap <Integer, Integer> bpt = createTree();
	Set<Map.Entry<Integer, Integer>> set = bpt.entrySet();
	
	assertEquals(10, set.size());
    }
        
    /**
     *
     * Tests the size method
     *
     */

    @Test
    
    public void testSize() {

	BpTreeMap <Integer, Integer> bpt = createTree();
	
	assertEquals(20, bpt.size());
    }

}