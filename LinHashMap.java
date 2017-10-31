
/************************************************************************************
 * @file LinHashMap.java
 *
 * @author  John Miller
 */

import java.io.*;
import java.lang.reflect.Array;
import static java.lang.System.out;
import java.util.*;


/************************************************************************************
 * This class provides hash maps that use the Linear Hashing algorithm.
 * A hash table is created that is an array of buckets.
 */

public class LinHashMap <K, V>
       extends AbstractMap <K, V>
       implements Serializable, Cloneable, Map <K, V>
{
    /** The number of slots (for key-value pairs) per bucket.
     */
    private static final int SLOTS = 4;

    /** The class for type K.
     */
    private final Class <K> classK;

    /** The class for type V.
     */
    private final Class <V> classV;

    /********************************************************************************
     * This inner class defines buckets that are stored in the hash table.
     */
    private class Bucket
    {
        int    nKeys;
        K []   key;
        V []   value;
        Bucket next;
        Bucket parent;

        @SuppressWarnings("unchecked")
        Bucket (Bucket n)
        {
            nKeys = 0;
            key   = (K []) Array.newInstance (classK, SLOTS);
            value = (V []) Array.newInstance (classV, SLOTS);
            next  = n;
            parent = null;
        } // constructor
    } // Bucket inner class

    /** The list of buckets making up the hash table.
     */
    private final List <Bucket> hTable;

    /** The modulus for low resolution hashing
     */
    private int mod1;

    /** The modulus for high resolution hashing
     */
    private int mod2;

    /** Counter for the number buckets accessed (for performance testing).
     */
    private int count = 0;

    /** The index of the next bucket to split.
     */
    private int split = 0;
    
    /********************************************************************************
     * Construct a hash table that uses Linear Hashing with 1 bucket.
     * @param classK    the class for keys (K)
     * @param classV    the class for keys (V)
     */
    public LinHashMap (Class <K> _classK, Class <V> _classV)
    {
        classK = _classK;
        classV = _classV;
        hTable = new ArrayList <Bucket> ();
        mod1   = 1;                        // initSize;
        mod2   = 2 * mod1;
        for(int i=0;i<1;i++){
        	Bucket initial=new Bucket(null);
        	hTable.add(initial);
        }
    } // constructor

    /********************************************************************************
     * Construct a hash table that uses Linear Hashing.
     * @param classK    the class for keys (K)
     * @param classV    the class for keys (V)
     * @param initSize  the initial number of home buckets (a power of 2, e.g., 4)
     */
    public LinHashMap (Class <K> _classK, Class <V> _classV, int initSize)
    {
        classK = _classK;
        classV = _classV;
        hTable = new ArrayList <Bucket> ();
        mod1   = initSize;                        // initSize;
        mod2   = 2 * mod1;
        for(int i=0;i<initSize;i++){
        	Bucket initial=new Bucket(null);
        	hTable.add(initial);
        }
    } // constructor

    /********************************************************************************
     * Return a set containing all the entries as pairs of keys and values.
     * @return  the set view of the map
     */
    public Set <Map.Entry <K, V>> entrySet ()
    {
        Set <Map.Entry <K, V>> enSet = new HashSet <> ();
        for(int i=0;i<hTable.size();i++){
        	enSet=addBucket(enSet,hTable.get(i));
        }

        //  T O   B E   I M P L E M E N T E D
            
        return enSet;
    } // entrySet
    
    public Set <Map.Entry <K,V>> addBucket(Set <Map.Entry <K,V>> curSet, Bucket currentBucket){
    	for(int i=0;i<currentBucket.nKeys;i++){
    		Map.Entry<K, V> entry=new AbstractMap.SimpleEntry<K, V>(currentBucket.key[i],currentBucket.value[i]);
    		curSet.add(entry);
    	}
    	if(currentBucket.next!=null){
    		curSet=addBucket(curSet,currentBucket.next);
    	}
    	return curSet;
    }

    /********************************************************************************
     * Given the key, look up the value in the hash table.
     * @param key  the key used for look up
     * @return  the value associated with the key
     */
    public V get (Object key)
    {
    	//  T O   B E   I M P L E M E N T E D
    	V ret;
        int i = h (key);
        if(split>i){
        	i = h2(key);
        }
        Bucket currentBucket=hTable.get(i);
        ret=getFromBucket(currentBucket,key);

        return ret;
    } // get
    
    /********************************************************************************
     * Retrieves values from the overflow bucket.
     * @param currentBucket 	the bucket to search
     * @param key 		the key to search for
     * @return the value of the key being searched for
     */
    public V getFromBucket(Bucket currentBucket, Object key){
    	V ret;
    	if(key instanceof KeyType){
    		for(int j=0;j<currentBucket.nKeys;j++){
    			KeyType k=(KeyType) key;
    			KeyType stored=(KeyType) currentBucket.key[j];
            	if(k.compareTo(stored)==0){
            		ret=currentBucket.value[j];
            		return ret;
            	}
            }
    	}
    	else{
	    	for(int j=0;j<currentBucket.nKeys;j++){
	        	if(currentBucket.key[j].equals(key)){
	        		ret=currentBucket.value[j];
	        		return ret;
	        	}
	        }
    	}
        if(currentBucket.next!=null){
        	ret=getFromBucket(currentBucket.next,key);
        }
        else{
        	ret=null;
        }

        return ret;
    }

    /********************************************************************************
     * Put the key-value pair in the hash table.
     * @param key    the key to insert
     * @param value  the value to insert
     * @return  null (not the previous value)
     */
    public V put (K key, V value)
    {
    	int i = h (key);
    	if(split>i){
    		i = h2(key);
    	}
    	Bucket currentBucket=hTable.get(i);
    	if(currentBucket.nKeys<SLOTS){
    		
    		currentBucket.key[currentBucket.nKeys]=key;
    		currentBucket.value[currentBucket.nKeys]=value;
    		currentBucket.nKeys++;
    	}
    	else{
			Bucket splitBucket=hTable.get(split);
			Bucket newBucket=new Bucket(null);
			hTable.add(newBucket);
			split++;
			reorganize(splitBucket);
    		if(split>i){
    			i = h2(key);
    		}
    		currentBucket=hTable.get(i);
    	    insertIntoCurrent(key,value,currentBucket);
    	    if(split>mod1-1){
        		split=0;
        		mod1=mod2;
        		mod2=2 * mod1;
        	}
    	}
    	
       // out.println ("LinearHashMap.put: key = " + key + ", h() = " + i + ", value = " + value);
        
        //  T O   B E   I M P L E M E N T E D
        return null;
    } // put
    
    /********************************************************************************
     * @param key	the key to be inserted
     * @param value the value to be inserted
     * @return null (not the previous value)
     */
    public V putNoSplit(K key,V value){
    	int i = h (key);
    	if(split>i){
    		i = h2(key);
    	}
    	Bucket currentBucket=hTable.get(i);
    	if(currentBucket.nKeys<SLOTS){
    		currentBucket.key[currentBucket.nKeys]=key;
    		currentBucket.value[currentBucket.nKeys]=value;
    		currentBucket.nKeys++;
    	}
    	else{
    		if(split>i){
    			i = h2(key);
    		}
    		currentBucket=hTable.get(i);
    	    insertIntoCurrent(key,value,currentBucket);
    	}
    	
        //out.println ("LinearHashMap.put: key = " + key + ", h() = " + i + ", value = " + value);
        
        //  T O   B E   I M P L E M E N T E D
        return null;
    }
    
    /********************************************************************************
     * Adds a key-value pair to an overflow bucket
     * @param key    the key to insert
     * @param value  the value to insert
     * @param currentBucket the current overflow bucket in the chain
     */
    public void insertIntoCurrent(K key,V value,Bucket currentBucket){
    	if(currentBucket.nKeys<SLOTS){
    		currentBucket.key[currentBucket.nKeys]=key;
    		currentBucket.value[currentBucket.nKeys]=value;
    		currentBucket.nKeys++;
    	}
    	else{
			if(currentBucket.next==null){
				currentBucket.next = new Bucket(null);
				currentBucket.next.parent = currentBucket;
			}
			insertIntoCurrent(key,value,currentBucket.next);
		}
    	
    }
    
    /********************************************************************************
     * Reorganizes the table after a new bucket is added
     */
    public void reorganize(Bucket splitBucket){
    	for(int j=0;j<splitBucket.nKeys;j++){
    		int currentMod=h(splitBucket.key[j]);
    		if(split>currentMod){
    			currentMod=h2(splitBucket.key[j]);
    		}
			Bucket moveBucket=hTable.get(currentMod);
			if(moveBucket!=splitBucket){
				putNoSplit(splitBucket.key[j],splitBucket.value[j]);
				ArrayList <K> keyCopy=new ArrayList <> (Arrays.asList(splitBucket.key)); 
				ArrayList <V> valueCopy=new ArrayList <> (Arrays.asList(splitBucket.value));
				keyCopy.remove(j);
				valueCopy.remove(j);
				splitBucket.key=keyCopy.toArray((K []) Array.newInstance (classK, SLOTS));
				splitBucket.value=valueCopy.toArray((V []) Array.newInstance (classV, SLOTS));				
				if(j<splitBucket.nKeys-1){
					j--;
				}
				splitBucket.nKeys--;
			}			
		}
    	if(splitBucket.next!=null){
			reorganizeOverflow(splitBucket,splitBucket.next);
		}
    }
    
    /********************************************************************************
     * Reorganizes overflow buckets after a split.
     * @param parent the parent bucket
     * @param overflow the current overflow bucket
     */
    public void reorganizeOverflow(Bucket parent,Bucket overflow){
    	for(int j=0;j<overflow.nKeys;j++){ 
			int currentMod=h(overflow.key[j]);
			if(split>currentMod){
				currentMod=h2(overflow.key[j]);
			}
			Bucket moveBucket=hTable.get(currentMod);
			Bucket originalParent=parent;
			while(originalParent.parent != null){
				originalParent = originalParent.parent;
			}
			if(moveBucket!=originalParent || parent.nKeys<SLOTS){
				putNoSplit(overflow.key[j],overflow.value[j]);
				ArrayList <K> keyCopy=new ArrayList <> (Arrays.asList(overflow.key)); 
				ArrayList <V> valueCopy=new ArrayList <> (Arrays.asList(overflow.value));
				keyCopy.remove(j);
				valueCopy.remove(j);
				overflow.key=keyCopy.toArray((K []) Array.newInstance (classK, SLOTS));
				overflow.value=valueCopy.toArray((V []) Array.newInstance (classV, SLOTS));	
				if(j<overflow.nKeys-1){
					j--;
				}				
				overflow.nKeys--;
				if(overflow.nKeys <= 0){
					break;
				}
			}
		}  
    	if(overflow.next!=null){
			reorganizeOverflow(overflow,overflow.next);
		}
    	if(overflow.nKeys==0){
			parent.next=null;
		}
    }

    /********************************************************************************
     * Return the size (SLOTS * number of home buckets) of the hash table. 
     * @return  the size of the hash table
     */
    public int size ()
    {
        return SLOTS * (mod1 + split);
    } // size

    /********************************************************************************
     * Print the hash table.
     */
    private void print ()
    {
        out.println ("Hash Table (Linear Hashing)");
        out.println ("-------------------------------------------");

        //  T O   B E   I M P L E M E N T E D	
        for(int i=0;i<hTable.size();i++){
        	out.print("Bucket "+i+": ");
        	Bucket currentBucket=hTable.get(i);
        	printBucket(currentBucket);        	        	        	
        }
        out.println ("-------------------------------------------");
    } // print
    
    /********************************************************************************
     * Prints the Bucket sent in
     * @param bucket the bucket to print
     */
    public void printBucket(Bucket currentBucket){
    	for(int j=0;j<currentBucket.nKeys;j++){
    		out.print("["+currentBucket.key[j]+","+currentBucket.value[j]+"]");
    		if(j<currentBucket.nKeys-1){
    			out.print(", ");
    		}
    	}
    	out.print("\n");
    	if(currentBucket.next!=null){
    		out.print("\t");
    		printBucket(currentBucket.next);
    	}
    }

    /********************************************************************************
     * Hash the key using the low resolution hash function.
     * @param key  the key to hash
     * @return  the location of the bucket chain containing the key-value pair
     */
    private int h (Object key)
    {
        return Math.abs(key.hashCode ()) % mod1;
    } // h

    /********************************************************************************
     * Hash the key using the high resolution hash function.
     * @param key  the key to hash
     * @return  the location of the bucket chain containing the key-value pair
     */
    private int h2 (Object key)
    {
        return Math.abs(key.hashCode ()) % mod2;
    } // h2

    /********************************************************************************
     * The main method used for testing.
     * @param  the command-line arguments (args [0] gives number of keys to insert)
     */
    public static void main (String [] args)
    {

        int totalKeys    = 320;
        boolean RANDOMLY = false;

        LinHashMap <Integer, Integer> ht = new LinHashMap <> (Integer.class, Integer.class, 4);
        if (args.length == 1) totalKeys = Integer.valueOf (args [0]);

        if (RANDOMLY) {
            Random rng = new Random ();
            for (int i = 1; i <= totalKeys; i += 1) ht.put (rng.nextInt (2 * totalKeys), i * 1);
        } else {
            for (int i = 1; i <= totalKeys; i += 1) ht.put (i, i * 1);
        } // if

        ht.print ();
        for (int i = 0; i <= totalKeys; i++) {
    		out.println ("key = " + i + " value = " + ht.get (i));
        } // for
        out.println ("-------------------------------------------");
        out.println ("Average number of buckets accessed = " + ht.count / (double) totalKeys);
    } // main

} // LinHashMap class
