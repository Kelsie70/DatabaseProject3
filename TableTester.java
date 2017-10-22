import static java.lang.System.out;
import static org.junit.Assert.*;

import org.junit.Test;


public class TableTester {
	
	/**
	 * 
	 * Creates a movie table for testing purposes. 
	 * Note that this is the same table that is created in
	 * MovieDB.java
	 * 
	 * @return a movie table
	 */

	public Table movieTable() {
		 Table movie = new Table ("movie", "title year length genre studioName producerNo",
                 "String Integer Integer String String Integer", "title year");
		 Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
	     Comparable [] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
	     Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
	     Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
	     movie.insert (film0);
	     movie.insert (film1);
	     movie.insert (film2);
	     movie.insert (film3);
	     
		 return movie;
	}
	
	/**
	 * 
	 * Creates a cinema table for testing purposes. 
	 * Note that this is the same table that is created in
	 * MovieDB.java
	 * 
	 * @return a cinema table
	 */
	
	public Table cinemaTable() {
		 Table cinema = new Table ("cinema", "title year length genre studioName producerNo",
                 "String Integer Integer String String Integer", "title year");
		 Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
	     Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
	     Comparable [] film4 = { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
	     cinema.insert (film2);
	     cinema.insert (film3);
	     cinema.insert (film4);
	     
		 return cinema;
	}
	
	/**
	 * 
	 * Creates a studio table for testing purposes. 
	 * Note that this is the same table that is created in
	 * MovieDB.java
	 * 
	 * @return a studio table
	 */
	
	public Table studioTable() {
		Table studio = new Table ("studio", "name address presNo",
                "String String Integer", "name");
		Comparable [] studio0 = { "Fox", "Los_Angeles", 7777 };
        Comparable [] studio1 = { "Universal", "Universal_City", 8888 };
        Comparable [] studio2 = { "DreamWorks", "Universal_City", 9999 };
        studio.insert (studio0);
        studio.insert (studio1);
        studio.insert (studio2);
        
        return studio;
	}
	
	/**
	 * Tests the project method with the created
	 * movie table 
	 * 
	 */
	@Test
	public void testProject()
	{
		Table movie = this.movieTable();
		Table movie_project = movie.project ("title year");
		
		assertEquals(0, movie_project.col("title"));
		assertEquals(1, movie_project.col("year"));
		assertEquals(-1, movie_project.col("length"));
	}
	
	/**
	 * Tests the select method with the created 
	 * movie table
	 * 
	 */
	@Test
	public void testSelect()
	{
		Table movie = this.movieTable();
		Table movie_select = movie.select(new KeyType("Star_Wars"));
		Comparable[] starWars = movie_select.getTuple(0);
		
		assertEquals(1, movie_select.tuplesLength());
		assertEquals("Star_Wars", starWars[0]);
	}
	
	/**
	 * Tests the union method with the created
	 * movie and cinema tables
	 * 
	 */
	@Test
	public void testUnion()
	{
		Table movie = this.movieTable();
		Table cinema = this.cinemaTable();
		Table union = movie.union(cinema);
		
		assertEquals(5, union.tuplesLength());
	}
	
	/**
	 * Tests the minus method with the created
	 * movie and cinema tables
	 * 
	 */
	@Test
	public void testMinus()
	{
		Table movie = this.movieTable();
		Table cinema = this.cinemaTable();
		Table minus = movie.minus(cinema);
	
		assertEquals(2, minus.tuplesLength());
	}
	
	/**
	 * Tests the equi join method with the created
	 * movie and studio tables
	 * 
	 */
	@Test
	public void testEquiJoin()
	{
		Table movie = this.movieTable();
		Table studio = this.studioTable();
		Table equiJoin = movie.join("studioName", "name", studio);
		
		Comparable studioName = equiJoin.getTuple(0)[equiJoin.col("studioName")];
		Comparable name = equiJoin.getTuple(0)[equiJoin.col("name")];
		
		assertEquals(0, studioName.compareTo(name));
	}
	
	/**
	 * Tests the natural join method with the created
	 * movie and cinema tables
	 * 
	 */
	@Test
	public void testNaturalJoin()
	{
		Table movie = this.movieTable();
		Table cinema = this.cinemaTable();
		Table naturalJoin = movie.join(cinema);
		
		assertEquals(2, naturalJoin.tuplesLength());
	}
}
