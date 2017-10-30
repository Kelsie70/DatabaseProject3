
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.System.out;
import java.util.HashMap;

/****************************************************************************************
 * This class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key. 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple number).
     */
    private final Map <KeyType, Comparable []> index;

    /** The supported map types.
     */
    private enum MapType { NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP }

    /** The map type to be used for indices.  Change as needed.
     */
    private static final MapType mType = MapType.LINHASH_MAP;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map <KeyType, Comparable []> makeMap ()
    {
        switch (mType) {
        case TREE_MAP:    return new TreeMap <> ();
        case LINHASH_MAP: return new LinHashMap <> (KeyType.class, Comparable [].class);
        case BPTREE_MAP:  return new BpTreeMap <> (KeyType.class, Comparable [].class);
        default:          return null;
        } // switch
    } // makeMap

    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = makeMap ();

    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = makeMap ();
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name       the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String attributes, String domains, String _key)
    {
        this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        String [] attrs     = attributes.split (" ");
        Class []  colDomain = extractDom (match (attrs), domain);
        String [] newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;

        List <Comparable []> rows = new ArrayList <> ();
        
        for(int i=0;i<attrs.length;i++){
            boolean attributeExists=false;
            for(int k=0;k<attribute.length;k++){
                if(attrs[i].equals(attribute[k])){
                    attributeExists=true;
                    break;
                }
            }
            if(!attributeExists){
                return null;
            }
        }
      
        for(int i=0;i<this.tuples.size();i++){
            Comparable[] projection=new Comparable[attrs.length];
            for(int j=0;j<this.tuples.get(i).length;j++){
                for(int k=0;k<attrs.length;k++){
                    projection[k]=this.tuples.get(i)[this.col(attrs[k])];
                }
            }
            rows.add(projection);
        }

        return new Table (name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();
        if(mType==MapType.LINHASH_MAP){
            rows.add(index.get(keyVal));
            if(rows.get(0)==null){
                rows.remove(0);
                rows.add(new Comparable[attribute.length]);
            }
        }
        else if(mType==MapType.TREE_MAP){
            for(int k=0;k<this.tuples.size();k++){
                for(int j=0;j<this.tuples.get(k).length;j++){
                    KeyType newKey = new KeyType (tuples.get(k)[j]);
                    if(newKey.toString().equals(keyVal.toString())){ 
                        rows.add(index.get(keyVal));
                    }
                }
            }
        }
        else if(mType==MapType.BPTREE_MAP) {
            rows.add(index.get(keyVal));
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal, int map)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();
        if(map == 0){ //linhash map
            rows.add(index.get(keyVal));
            if(rows.get(0)==null){
                rows.remove(0);
                rows.add(new Comparable[attribute.length]);
            }
        }
        else if(map == 1){ //tree map
            for(int k=0;k<this.tuples.size();k++){
                for(int j=0;j<this.tuples.get(k).length;j++){
                    KeyType newKey = new KeyType (tuples.get(k)[j]);
                    if(newKey.toString().equals(keyVal.toString())){ 
                        rows.add(index.get(keyVal));
                    }
                }
            }
        }
        else if(map == 2) { //bptree map
            rows.add(index.get(keyVal));
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (keyval1 <= value < keyval2).
     * Use an B+ Tree index (SortedMap) to retrieve the tuples with keys in the given range.
     *
     * @param keyVal1  the given lower bound for the range (inclusive)
     * @param keyVal2  the given upper bound for the range (exclusive)
     * @return  a table with the tuples satisfying the key predicate
     */
    public Table select (KeyType keyVal1, KeyType keyVal2)
    {
        out.println ("RA> " + name + ".select between (" + keyVal1 + ") and " + keyVal2);

        List <Comparable []> rows = new ArrayList <> ();

        Set<Map.Entry<KeyType,Comparable[]>> entries = index.entrySet();

        boolean range = false;
        for(Map.Entry<KeyType,Comparable[]> e : entries) {
            if(e.getKey().compareTo(keyVal1) == 0) {
                range = true;
            }
            if(range) {
                rows.add(index.get(e.getKey()));
            }
            if(e.getKey().compareTo(keyVal2) == 0) {
                range = false;
            }
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // range_select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        for(int i=0;i<tuples.size();i++){
            rows.add(tuples.get(i));
        }
        for(int i=0;i<table2.tuples.size();i++){
            boolean inBoth=false;
            for(int k=0;k<tuples.size();k++){
                boolean equalArray=true;
                for(int j=0;j<tuples.get(k).length;j++){
                    if(!tuples.get(k)[j].equals(table2.tuples.get(i)[j])){
                        equalArray=false;
                        break;
                    }
                    inBoth=equalArray;
                    if(equalArray){                     
                        break;
                    }
                }
            }
            if(!inBoth){
                rows.add(table2.tuples.get(i));
            }
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        for(int i=0;i<tuples.size();i++){
            boolean inBoth=false;
            for(int k=0;k<table2.tuples.size();k++){
                boolean equalArray=true;
                for(int j=0;j<table2.tuples.get(k).length;j++){
                    if(!table2.tuples.get(k)[j].equals(tuples.get(i)[j])){
                        equalArray=false;
                        break;
                    }
                    inBoth=equalArray;
                    if(equalArray){                     
                        break;
                    }
                }
            }
            if(!inBoth){
                rows.add(tuples.get(i));
            }
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.  Implement using
     * a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");

        List <Comparable []> rows = new ArrayList <> ();

        boolean attributeOneExists=false;
        boolean attributeTwoExists=false;
        for(int i=0;i<attribute.length;i++){
            if(attributes1.equals(attribute[i])){
                attributeOneExists=true;
            }
        }
        for(int i=0;i<table2.attribute.length;i++){
            if(attributes2.equals(table2.attribute[i])){
                attributeTwoExists=true;
            }
        }
        if(!attributeOneExists || !attributeTwoExists){
            return null;
        }
        for(int i=0;i<tuples.size();i++)
        {
            for(int j=0;j<table2.tuples.size();j++){
                if(table2.tuples.get(j)[table2.col(attributes2)].equals(tuples.get(i)[this.col(attributes1)])){
                    Comparable[] entry=new Comparable[table2.tuples.get(j).length+tuples.get(i).length];
                    int index=0;
                    for(int l=0;l<tuples.get(i).length;l++){
                        entry[index]=tuples.get(i)[this.col(this.attribute[l])];
                        index++;
                    }
                    for(int l=0;l<table2.tuples.get(j).length;l++){
                            entry[index]=table2.tuples.get(j)[table2.col(table2.attribute[l])];
                            index++;
                    }
                    rows.add(entry);
                }
            }
        }

        return new Table (name + count++, ArrayUtil.concat (attribute, table2.attribute),
                                          ArrayUtil.concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using an Index Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table i_join (String attributes1, String attributes2, Table table2)
    {       
        if(mType==MapType.LINHASH_MAP){
            LinHashMap <KeyType, Comparable[]> ht = new LinHashMap <> (KeyType.class, Comparable [].class);
            String[] keyArrayT1=attributes1.split(" ");
            String[] keyArrayT2=attributes2.split(" ");
            List <Comparable[]> rows = new ArrayList <> ();     
            for(Comparable[] b : table2.tuples){
                ArrayList<Comparable> valuesB=new ArrayList<Comparable>(); 
                ArrayList<Integer> indexB=new ArrayList<Integer>();
                Comparable [] keyVal=new Comparable[keyArrayT2.length];
                for(int j=0;j<keyArrayT2.length;j++){
                    int index=Arrays.asList(table2.attribute).indexOf(keyArrayT2[j]);
                    if(index==-1){
                        return null;
                    }
                    keyVal[j]=b[index];
                    indexB.add(index);
                }
                Collections.sort(indexB,Collections.reverseOrder());
                List<Comparable> copiedAttributes=new LinkedList<Comparable>(Arrays.asList(b));
                for(Integer i: indexB){
                    int index=i;
                    copiedAttributes.remove(index);
                    int length=b.length-keyArrayT2.length;
                    b=copiedAttributes.toArray(new Comparable[length]);
                }
                ht.put(new KeyType(keyVal), b);
            }
            for(Comparable[] a : tuples){
                Comparable [] keyVal=new Comparable[keyArrayT1.length];
                for(int j=0;j<keyArrayT1.length;j++){
                    int index=Arrays.asList(attribute).indexOf(keyArrayT1[j]);
                    if(index==-1){
                        return null;
                    }
                    keyVal[j]=a[index];
                }
                Comparable[] b=ht.get(new KeyType(keyVal));
                ArrayList<Comparable> joinedRow=new ArrayList<Comparable>();
                Comparable[] fullRow=ArrayUtil.concat(a, b);
                for(Comparable c:fullRow){
                    joinedRow.add(c);
                }
                int length=attribute.length+table2.attribute.length-1;
                Comparable[] completedRow=joinedRow.toArray(new Comparable[length]);
                rows.add(completedRow);
            }
            List<String> updatedAttributes=new LinkedList<String>(Arrays.asList(table2.attribute));
            for(String s: keyArrayT2){
                int index=Arrays.asList(table2.attribute).indexOf(s);
                updatedAttributes.remove(index);            
            }
            String[] updatedAttributesArray=updatedAttributes.toArray(new String[table2.attribute.length-keyArrayT2.length]);
            if(rows.size()==0){
                rows.add(new Comparable[attribute.length]);
            }
            return new Table (name + count++, ArrayUtil.concat (attribute, updatedAttributesArray),
                    ArrayUtil.concat (domain, table2.domain), key, rows);
        }
    else if(mType == MapType.TREE_MAP){
        TreeMap<KeyType,List<Comparable[]>> treemapJoin= new TreeMap<KeyType,List <Comparable[]>>();  
        String[] keyArrayT1=attributes1.split(" ");
        String[] keyArrayT2=attributes2.split(" ");
        for(Comparable[] b : table2.tuples){
            ArrayList<Comparable> valuesB =new ArrayList<Comparable>(); 
            ArrayList<Integer> indexB =new ArrayList<Integer>();
            for(String s:keyArrayT2){
                int index=Arrays.asList(table2.attribute).indexOf(s);
                if(index==-1){
                    return null;
                }
                valuesB.add(b[index]);
                indexB.add(index);
            }
            Collections.sort(indexB,Collections.reverseOrder());
            List<Comparable> copiedAttributes=new LinkedList<Comparable>(Arrays.asList(b));
            for(Integer i: indexB){
                int index=i;
                copiedAttributes.remove(index);
                int length=b.length-keyArrayT2.length;
                b=copiedAttributes.toArray(new Comparable[length]);
            }
            Comparable[] valuesArrayB=valuesB.toArray(new Comparable[keyArrayT1.length]);
            KeyType keyB=new KeyType(valuesArrayB);
            if(treemapJoin.get(keyB)==null){
                List<Comparable[]> value=new ArrayList<Comparable[]>();
                value.add(b);
                treemapJoin.put(keyB, value);
            }
            else{
                List<Comparable[]> duplicateKey=treemapJoin.get(keyB);
                duplicateKey.add(b);
                treemapJoin.put(keyB,duplicateKey);
            }
            
        }
        List <Comparable[]> rows = new ArrayList <> ();
        for(Comparable[]a: tuples){
            ArrayList<Comparable> valuesA=new ArrayList<Comparable>();          
            for(String s:keyArrayT1){
                int index=Arrays.asList(this.attribute).indexOf(s);
                if(index==-1){
                    return null;
                }
                valuesA.add(a[index]);
            }
            Comparable[] valuesArrayA=valuesA.toArray(new Comparable[keyArrayT1.length]);
            KeyType keyA=new KeyType(valuesArrayA);
            Comparable[] b=treemapJoin.get(keyA).get(0);
            if(treemapJoin.get(keyA).size()>1){
                List<Comparable[]> removeDuplicateKeys=treemapJoin.get(keyA);
                removeDuplicateKeys.remove(0);
                treemapJoin.put(keyA,removeDuplicateKeys);
            }
            ArrayList<Comparable> joinedRow=new ArrayList<Comparable>();
            Comparable[] fullRow=ArrayUtil.concat(a, b);
            for(Comparable c:fullRow){
                joinedRow.add(c);
            }
            int length=attribute.length+table2.attribute.length-1;
            Comparable[] completedRow=joinedRow.toArray(new Comparable[length]);
            rows.add(completedRow);
        }
        
        List<String> updatedAttributes=new LinkedList<String>(Arrays.asList(table2.attribute));
        for(String s: keyArrayT2){
            int index=Arrays.asList(table2.attribute).indexOf(s);
            updatedAttributes.remove(index);            
        }
        String[] updatedAttributesArray=updatedAttributes.toArray(new String[table2.attribute.length-keyArrayT2.length]);
        if(rows.size()==0){
            return null;
        }
        return new Table (name + count++, ArrayUtil.concat (attribute, updatedAttributesArray),
                ArrayUtil.concat (domain, table2.domain), key, rows);
        
    }
    else if(mType == MapType.BPTREE_MAP) {
        BpTreeMap <KeyType, Comparable[]> bp = new BpTreeMap <> (KeyType.class, Comparable [].class);
            String[] keyArrayT1=attributes1.split(" ");
            String[] keyArrayT2=attributes2.split(" ");
            List <Comparable[]> rows = new ArrayList <> ();     
            for(Comparable[] b : table2.tuples){
                ArrayList<Comparable> valuesB=new ArrayList<Comparable>(); 
                ArrayList<Integer> indexB=new ArrayList<Integer>();
                Comparable [] keyVal=new Comparable[keyArrayT2.length];
                for(int j=0;j<keyArrayT2.length;j++){
                    int index=Arrays.asList(table2.attribute).indexOf(keyArrayT2[j]);
                    if(index==-1){
                        return null;
                    }
                    keyVal[j]=b[index];
                    indexB.add(index);
                }
                Collections.sort(indexB,Collections.reverseOrder());
                List<Comparable> copiedAttributes=new LinkedList<Comparable>(Arrays.asList(b));
                for(Integer i: indexB){
                    int index=i;
                    copiedAttributes.remove(index);
                    int length=b.length-keyArrayT2.length;
                    b=copiedAttributes.toArray(new Comparable[length]);
                }
                bp.put(new KeyType(keyVal), b);
            }
            for(Comparable[] a : tuples){
                Comparable [] keyVal=new Comparable[keyArrayT1.length];
                for(int j=0;j<keyArrayT1.length;j++){
                    int index=Arrays.asList(attribute).indexOf(keyArrayT1[j]);
                    if(index==-1){
                        return null;
                    }
                    keyVal[j]=a[index];
                }
                Comparable[] b=bp.get(new KeyType(keyVal));
                ArrayList<Comparable> joinedRow=new ArrayList<Comparable>();
                Comparable[] fullRow=ArrayUtil.concat(a, b);
                for(Comparable c:fullRow){
                    joinedRow.add(c);
                }
                int length=attribute.length+table2.attribute.length-1;
                Comparable[] completedRow=joinedRow.toArray(new Comparable[length]);
                rows.add(completedRow);
            }
            List<String> updatedAttributes=new LinkedList<String>(Arrays.asList(table2.attribute));
            for(String s: keyArrayT2){
                int index=Arrays.asList(table2.attribute).indexOf(s);
                updatedAttributes.remove(index);            
            }
            String[] updatedAttributesArray=updatedAttributes.toArray(new String[table2.attribute.length-keyArrayT2.length]);
            if(rows.size()==0){
                rows.add(new Comparable[attribute.length]);
            }
            return new Table (name + count++, ArrayUtil.concat (attribute, updatedAttributesArray),
                    ArrayUtil.concat (domain, table2.domain), key, rows);
    }
        return null;
    } // i_join
    
    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using an Index Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @param map         the map to use for index join
     * @return  a table with tuples satisfying the equality predicate
     */
    
    public Table i_join (String attributes1, String attributes2, Table table2, int map)
    {   
        if(map == 0){
            TreeMap<KeyType,List<Comparable[]>> treemapJoin= new TreeMap<KeyType,List <Comparable[]>>();  
            String[] keyArrayT1=attributes1.split(" ");
            String[] keyArrayT2=attributes2.split(" ");
            Table table1=this;
            if(Arrays.equals(keyArrayT1, this.key) && !Arrays.equals(keyArrayT2, table2.key)){
            	Table temp;
            	String[] stringTemp;
            	temp=table1;
            	table1=table2;
            	table2=temp;
            	stringTemp=keyArrayT1;
            	keyArrayT1=keyArrayT2;
            	keyArrayT2=stringTemp;
            }
            for(Comparable[] b : table2.tuples){
                ArrayList<Comparable> valuesB =new ArrayList<Comparable>(); 
                ArrayList<Integer> indexB =new ArrayList<Integer>();
                for(String s:keyArrayT2){
                    int index=Arrays.asList(table2.attribute).indexOf(s);
                    if(index==-1){
                        return null;
                    }
                    valuesB.add(b[index]);
                    indexB.add(index);
                }
                Collections.sort(indexB,Collections.reverseOrder());
                List<Comparable> copiedAttributes=new LinkedList<Comparable>(Arrays.asList(b));
                for(Integer i: indexB){
                    int index=i;
                    copiedAttributes.remove(index);
                    int length=b.length-keyArrayT2.length;
                    b=copiedAttributes.toArray(new Comparable[length]);
                }
                Comparable[] valuesArrayB=valuesB.toArray(new Comparable[keyArrayT1.length]);
                KeyType keyB=new KeyType(valuesArrayB);
                if(treemapJoin.get(keyB)==null){
                    List<Comparable[]> value=new ArrayList<Comparable[]>();
                    value.add(b);
                    treemapJoin.put(keyB, value);
                }
                else{
                    List<Comparable[]> duplicateKey=treemapJoin.get(keyB);
                    duplicateKey.add(b);
                    treemapJoin.put(keyB,duplicateKey);
                }
                
            }
            List <Comparable[]> rows = new ArrayList <> ();
            for(Comparable[]a: table1.tuples){
                ArrayList<Comparable> valuesA=new ArrayList<Comparable>();          
                for(String s:keyArrayT1){
                    int index=Arrays.asList(table1.attribute).indexOf(s);
                    if(index==-1){
                        return null;
                    }
                    valuesA.add(a[index]);
                }
                Comparable[] valuesArrayA=valuesA.toArray(new Comparable[keyArrayT1.length]);
                KeyType keyA=new KeyType(valuesArrayA);
                Comparable[] b=treemapJoin.get(keyA).get(0);
                if(treemapJoin.get(keyA).size()>1){
                    List<Comparable[]> removeDuplicateKeys=treemapJoin.get(keyA);
                    removeDuplicateKeys.remove(0);
                    treemapJoin.put(keyA,removeDuplicateKeys);
                }
                ArrayList<Comparable> joinedRow=new ArrayList<Comparable>();
                Comparable[] fullRow=ArrayUtil.concat(a, b);
                for(Comparable c:fullRow){
                    joinedRow.add(c);
                }
                int length=table1.attribute.length+table2.attribute.length-1;
                Comparable[] completedRow=joinedRow.toArray(new Comparable[length]);
                rows.add(completedRow);
            }
            
            List<String> updatedAttributes=new LinkedList<String>(Arrays.asList(table2.attribute));
            for(String s: keyArrayT2){
                int index=Arrays.asList(table2.attribute).indexOf(s);
                updatedAttributes.remove(index);            
            }
            String[] updatedAttributesArray=updatedAttributes.toArray(new String[table2.attribute.length-keyArrayT2.length]);
            if(rows.size()==0){
                return null;
            }
            return new Table (table1.name + table1.count++, ArrayUtil.concat (table1.attribute, updatedAttributesArray),
                    ArrayUtil.concat (table1.domain, table2.domain), table1.key, rows);
            
        }
        else if(map == 1){
            LinHashMap <KeyType, Comparable[]> ht = new LinHashMap <> (KeyType.class, Comparable [].class);
            String[] keyArrayT1=attributes1.split(" ");
            String[] keyArrayT2=attributes2.split(" ");
            Table table1=this;
            if(Arrays.equals(keyArrayT1, this.key) && !Arrays.equals(keyArrayT2, table2.key)){
            	Table temp;
            	String[] stringTemp;
            	temp=table1;
            	table1=table2;
            	table2=temp;
            	stringTemp=keyArrayT1;
            	keyArrayT1=keyArrayT2;
            	keyArrayT2=stringTemp;
            }
            List <Comparable[]> rows = new ArrayList <> ();     
            for(Comparable[] b : table2.tuples){
                ArrayList<Comparable> valuesB=new ArrayList<Comparable>(); 
                ArrayList<Integer> indexB=new ArrayList<Integer>();
                Comparable [] keyVal=new Comparable[keyArrayT2.length];
                for(int j=0;j<keyArrayT2.length;j++){
                    int index=Arrays.asList(table2.attribute).indexOf(keyArrayT2[j]);
                    if(index==-1){
                        return null;
                    }
                    keyVal[j]=b[index];
                    indexB.add(index);
                }
                Collections.sort(indexB,Collections.reverseOrder());
                List<Comparable> copiedAttributes=new LinkedList<Comparable>(Arrays.asList(b));
                for(Integer i: indexB){
                    int index=i;
                    copiedAttributes.remove(index);
                    int length=b.length-keyArrayT2.length;
                    b=copiedAttributes.toArray(new Comparable[length]);
                }
                ht.put(new KeyType(keyVal), b);
            }
            for(Comparable[] a : table1.tuples){
                Comparable [] keyVal=new Comparable[keyArrayT1.length];
                for(int j=0;j<keyArrayT1.length;j++){
                    int index=Arrays.asList(table1.attribute).indexOf(keyArrayT1[j]);
                    if(index==-1){
                        return null;
                    }
                    keyVal[j]=a[index];
                }
                Comparable[] b=ht.get(new KeyType(keyVal));
                ArrayList<Comparable> joinedRow=new ArrayList<Comparable>();
                Comparable[] fullRow=ArrayUtil.concat(a, b);
                for(Comparable c:fullRow){
                    joinedRow.add(c);
                }
                int length=table1.attribute.length+table2.attribute.length-1;
                Comparable[] completedRow=joinedRow.toArray(new Comparable[length]);
                rows.add(completedRow);
            }
            List<String> updatedAttributes=new LinkedList<String>(Arrays.asList(table2.attribute));
            for(String s: keyArrayT2){
                int index=Arrays.asList(table2.attribute).indexOf(s);
                updatedAttributes.remove(index);            
            }
            String[] updatedAttributesArray=updatedAttributes.toArray(new String[table2.attribute.length-keyArrayT2.length]);
            if(rows.size()==0){
                rows.add(new Comparable[table1.attribute.length]);
            }
            return new Table (table1.name + table1.count++, ArrayUtil.concat (table1.attribute, updatedAttributesArray),
                    ArrayUtil.concat (table1.domain, table2.domain), table1.key, rows);
    }
    else if(map == 2) {
        BpTreeMap <KeyType, Comparable[]> bp = new BpTreeMap <> (KeyType.class, Comparable [].class);
            String[] keyArrayT1=attributes1.split(" ");
            String[] keyArrayT2=attributes2.split(" ");
            Table table1=this;
            if(Arrays.equals(keyArrayT1, this.key) && !Arrays.equals(keyArrayT2, table2.key)){
            	Table temp;
            	String[] stringTemp;
            	temp=table1;
            	table1=table2;
            	table2=temp;
            	stringTemp=keyArrayT1;
            	keyArrayT1=keyArrayT2;
            	keyArrayT2=stringTemp;
            }
            List <Comparable[]> rows = new ArrayList <> ();     
            for(Comparable[] b : table2.tuples){
                ArrayList<Comparable> valuesB=new ArrayList<Comparable>(); 
                ArrayList<Integer> indexB=new ArrayList<Integer>();
                Comparable [] keyVal=new Comparable[keyArrayT2.length];
                for(int j=0;j<keyArrayT2.length;j++){
                    int index=Arrays.asList(table2.attribute).indexOf(keyArrayT2[j]);
                    if(index==-1){
                        return null;
                    }
                    keyVal[j]=b[index];
                    indexB.add(index);
                }
                Collections.sort(indexB,Collections.reverseOrder());
                List<Comparable> copiedAttributes=new LinkedList<Comparable>(Arrays.asList(b));
                for(Integer i: indexB){
                    int index=i;
                    copiedAttributes.remove(index);
                    int length=b.length-keyArrayT2.length;
                    b=copiedAttributes.toArray(new Comparable[length]);
                }
                bp.put(new KeyType(keyVal), b);
            }
            for(Comparable[] a : table1.tuples){
                Comparable [] keyVal=new Comparable[keyArrayT1.length];
                for(int j=0;j<keyArrayT1.length;j++){
                    int index=Arrays.asList(table1.attribute).indexOf(keyArrayT1[j]);
                    if(index==-1){
                        return null;
                    }
                    keyVal[j]=a[index];
                }
                Comparable[] b=bp.get(new KeyType(keyVal));
                ArrayList<Comparable> joinedRow=new ArrayList<Comparable>();
                Comparable[] fullRow=ArrayUtil.concat(a, b);
                for(Comparable c:fullRow){
                    joinedRow.add(c);
                }
                int length=table1.attribute.length+table2.attribute.length-1;
                Comparable[] completedRow=joinedRow.toArray(new Comparable[length]);
                rows.add(completedRow);
            }
            List<String> updatedAttributes=new LinkedList<String>(Arrays.asList(table2.attribute));
            for(String s: keyArrayT2){
                int index=Arrays.asList(table2.attribute).indexOf(s);
                updatedAttributes.remove(index);            
            }
            String[] updatedAttributesArray=updatedAttributes.toArray(new String[table2.attribute.length-keyArrayT2.length]);
            if(rows.size()==0){
                rows.add(new Comparable[table1.attribute.length]);
            }
            return new Table (table1.name + table1.count++, ArrayUtil.concat (table1.attribute, updatedAttributesArray),
                    ArrayUtil.concat (table1.domain, table2.domain), table1.key, rows);
    }
        return null;
    } // i_join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using a Hash Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table h_join (String attributes1, String attributes2, Table table2)
    {
        HashMap <KeyType, Comparable[]> ht = new HashMap<KeyType, Comparable[]>();  
        String[] keyArrayT1=attributes1.split(" ");
        String[] keyArrayT2=attributes2.split(" ");
        Table table1=this;
        if(Arrays.equals(keyArrayT1, this.key) && !Arrays.equals(keyArrayT2, table2.key)){
        	Table temp;
        	String[] stringTemp;
        	temp=table1;
        	table1=table2;
        	table2=temp;
        	stringTemp=keyArrayT1;
        	keyArrayT1=keyArrayT2;
        	keyArrayT2=stringTemp;
        }
        List <Comparable[]> rows = new ArrayList <> ();     
        for(Comparable[] b : table2.tuples){
            ArrayList<Comparable> valuesB=new ArrayList<Comparable>(); 
            ArrayList<Integer> indexB=new ArrayList<Integer>();
            Comparable [] keyVal=new Comparable[keyArrayT2.length];
            for(int j=0;j<keyArrayT2.length;j++){
                int index=Arrays.asList(table2.attribute).indexOf(keyArrayT2[j]);
                if(index==-1){
                    return null;
                }
                keyVal[j]=b[index];
                indexB.add(index);
            }
            Collections.sort(indexB,Collections.reverseOrder());
            List<Comparable> copiedAttributes=new LinkedList<Comparable>(Arrays.asList(b));
            for(Integer i: indexB){
                int index=i;
                copiedAttributes.remove(index);
                int length=b.length-keyArrayT2.length;
                b=copiedAttributes.toArray(new Comparable[length]);
            }
            ht.put(new KeyType(keyVal), b);
        }
        for(Comparable[] a : table1.tuples){
            Comparable [] keyVal=new Comparable[keyArrayT1.length];
            for(int j=0;j<keyArrayT1.length;j++){
                int index=Arrays.asList(table1.attribute).indexOf(keyArrayT1[j]);
                if(index==-1){
                    return null;
                }
                keyVal[j]=a[index];
            }
            Comparable[] b=ht.get(new KeyType(keyVal));
            ArrayList<Comparable> joinedRow=new ArrayList<Comparable>();
            Comparable[] fullRow=ArrayUtil.concat(a, b);
            for(Comparable c:fullRow){
                joinedRow.add(c);
            }
            int length=table1.attribute.length+table2.attribute.length-1;
            Comparable[] completedRow=joinedRow.toArray(new Comparable[length]);
            rows.add(completedRow);
        }
        List<String> updatedAttributes=new LinkedList<String>(Arrays.asList(table2.attribute));
        for(String s: keyArrayT2){
            int index=Arrays.asList(table2.attribute).indexOf(s);
            updatedAttributes.remove(index);            
        }
        String[] updatedAttributesArray=updatedAttributes.toArray(new String[table2.attribute.length-keyArrayT2.length]);
        if(rows.size()==0){
            rows.add(new Comparable[table1.attribute.length]);
        }
        return new Table (table1.name + table1.count++, ArrayUtil.concat (table1.attribute, updatedAttributesArray),
                ArrayUtil.concat (table1.domain, table2.domain), table1.key, rows);
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        List <Comparable []> rows = new ArrayList <> ();


        ArrayList<Integer> equalPos = new ArrayList<Integer>();
        ArrayList<Integer> equalPos2 = new ArrayList<Integer>();

        for(int i = 0; i < attribute.length; i++) {
            for(int j = 0; j < table2.attribute.length; j++) {
                if(attribute[i].equals(table2.attribute[j])) {
                    equalPos.add(i);
                    equalPos2.add(j);
                }
            }
        }   //finds where matching attributes are

        if(equalPos.size() != 0) { //if they share any attributes
            for(int i = 0; i < tuples.size(); i++) { //iterate through table1 rows
                for(int j = 0; j < table2.tuples.size(); j++) { //iterate through table2 rows
                    for(int k = 0; k < equalPos.size(); k++) { //iterate through matching columsn
                        if(tuples.get(i)[equalPos.get(k)].equals(table2.tuples.get(j)[equalPos2.get(k)]) && k%equalPos.size() == 0) { //checks to see if values match
                            Comparable[] entry = new Comparable[table2.tuples.get(j).length+tuples.get(i).length-equalPos.size()];
                            int index = 0;
                        
                            for(int l=0;l < tuples.get(i).length;l++) {
                                entry[index] = tuples.get(i)[this.col(this.attribute[l])];
                                index++;
                            }
                            while(index != table2.tuples.get(j).length+tuples.get(i).length-equalPos.size()) {  //adds table 2 columns if necessary
                                int l = 0;
                                if(!equalPos.contains(table2.col(table2.attribute[l]))) {
                                    entry[index] = table2.tuples.get(j)[table2.col(table2.attribute[l])];
                                    index++;
                                }
                                l++;
                            }
                            rows.add(entry);
                        }
                    }
                }
            }

            String [] updatedAttribute = new String[rows.get(0).length - attribute.length];
            if((rows.get(0).length - attribute.length) != 0) {
                for(String attr:table2.attribute) {
                    int i = 0;
                    if(!Arrays.asList(attribute).contains(attr)) {
                        updatedAttribute[i] = attr;
                    } 
                    i++;
                }
            }  
            // FIX - eliminate duplicate columns
            return new Table (name + count++, ArrayUtil.concat (attribute, updatedAttribute),
                                              ArrayUtil.concat (domain, table2.domain), key, rows);
        }
        else {
            return new Table (name + count++, ArrayUtil.concat (attribute, table2.attribute),
                                              ArrayUtil.concat (domain, table2.domain), key, rows);
        }
    } // join

    /************************************************************************************
     * Return the column position for the given attribute name.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (int i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;  // not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        //out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            Comparable [] keyVal = new Comparable [key.length];
            int []        cols   = match (key);
            for (int j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];            
            if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);           
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        out.print ("| ");
        for (String a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        for (Comparable [] tup : tuples) {
            out.print ("| ");
            for (Comparable attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        if (mType != MapType.NO_MAP) {
            for (Map.Entry <KeyType, Comparable []> e : index.entrySet ()) {
                out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
            } // for
        } // if
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            ObjectOutputStream oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (int j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (int j = 0; j < column.length; j++) {
            boolean matched = false;
            for (int k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        Comparable [] tup = new Comparable [column.length];
        int [] colPos = match (column);
        for (int j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in list) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a list of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 
        //  T O   B E   I M P L E M E N T E D 

        return true;
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        Class [] classArray = new Class [className.length];

        for (int i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group  where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        Class [] obj = new Class [colPos.length];

        for (int j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom

    /************************************************************************************
     * Returns the length of the table.
     *
     * @return  the tuple length of the table
     */
    
    public int tuplesLength() {
    return tuples.size();
    }
    
    /************************************************************************************
     * Returns the tuple at the index.
     *
     * @param i the index of the tuple
     * @return  the tuple at the index
     */
    
    public Comparable[] getTuple(int i) {
    return tuples.get(i);
    }

} // Table class

