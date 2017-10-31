public class PerformanceTest {

  public static void main(String[] args) {
	  for(int n=20;n<=1280;n*=2){
		 Table[] tableArray = fillTables(n);
		 System.out.println("\n----------Number of Tuples: " + n + "----------");
	
	     //-----------Select Tests---------------
	
	     System.out.println("---------Select---------");
	     long start = System.nanoTime();
	     //Sequential Select status
	     Table selectTable = tableArray[0].select(t -> t[tableArray[0].col("status")].equals("930409"));
	     long end = System.nanoTime();
	     double duration = (end - start)/1000000.0;
	     System.out.println("Sequential Select\nTime: " + duration + " ms");
	
         start=System.nanoTime();
         //Index Select status
         selectTable = tableArray[0].select(new KeyType(930409));
         end = System.nanoTime();
         duration = (end - start)/1000000.0;
         System.out.println("Index Select");
         System.out.println("Time: " + duration + " ms\n");
	    
	
	     //-----------Range Select Tests---------------
	
	     System.out.println("---------Range Select---------");
	     double  timeStart;
	     double timeEnd;
	     double finalTime; 

    	 Table testValues;
    	 
    	 //Sequential Range Select
    	 timeStart = System.nanoTime();
		 tableArray[0].select(test -> test[0].compareTo(930390) >= 0 && test[0].compareTo(930409) <= 0);
		 timeEnd = System.nanoTime();
		 finalTime = (timeEnd - timeStart) / 1000000.0;
		 System.out.println("Sequential Range Select");
		 System.out.println("Time: " + finalTime + " ms");
		 
		 //Index Range Select
		 timeStart = System.nanoTime();
		 testValues = tableArray[0].select(new KeyType(930390), new KeyType(930409));
	     timeEnd = System.nanoTime();
	     finalTime = (timeEnd - timeStart) / 1000000.0;
	     System.out.println("Index Range Select");
	     System.out.println("Time: " + finalTime + " ms\n");
	
	      //-----------Join Tests-----------------
	
	      System.out.println("---------Join---------");
	      start = System.nanoTime();
	      //Nested Join Student to Transcript
	      Table joinTable = tableArray[0].join("id", "studId", tableArray[4]);
	      end = System.nanoTime();
	      duration = (end - start)/1000000.0;
	      System.out.println("Nested Loop Join\nTime: " + duration + " ms");
	    
	      start=System.nanoTime();
	      //Hash Join Student to Transcript
	      joinTable = tableArray[4].h_join("studId", "id", tableArray[0]);
	      end = System.nanoTime();
	      duration = (end - start)/1000000.0;
	      System.out.println("Hash Join\nTime: " + duration + " ms");
	    
	      start=System.nanoTime();
	      //Index Join Student to Transcript
	      joinTable = tableArray[4].i_join("studId", "id", tableArray[0]);
	      end = System.nanoTime();
	      duration = (end - start)/1000000.0;
	      System.out.println("Index Join");
	      System.out.println("Time: " + duration + " ms\n");
	      
      }
  }

  public static Table[] fillTables(int num){
    TupleGenerator test = new TupleGeneratorImpl ();

        test.addRelSchema ("Student",
                           "id name address status",
                           "Integer String String String",
                           "id",
                           null);
        
        test.addRelSchema ("Professor",
                           "id name deptId",
                           "Integer String String",
                           "id",
                           null);
        
        test.addRelSchema ("Course",
                           "crsCode deptId crsName descr",
                           "String String String String",
                           "crsCode",
                           null);
        
        test.addRelSchema ("Teaching",
                           "crsCode semester profId",
                           "String String Integer",
                           "crcCode semester",
                           new String [][] {{ "profId", "Professor", "id" },
                                            { "crsCode", "Course", "crsCode" }});
        
        test.addRelSchema ("Transcript",
                           "studId crsCode semester grade",
                           "Integer String String String",
                           "studId crsCode semester",
                           new String [][] {{ "studId", "Student", "id"},
                                            { "crsCode", "Course", "crsCode" },
                                            { "crsCode semester", "Teaching", "crsCode semester" }});

        
        int tups [] = new int [] { num, num, num, num, num };
    
        Comparable [][][] resultTest = test.generate (tups);
         
        Table students = new Table("Student", "id name address status", "Integer String String String", "id");
        Table professor = new Table("Professor", "id name deptId", "Integer String String","id");
        Table course = new Table("Course", "crsCode deptId crsName descr", "String String String String", "crsCode");
        Table teaching = new Table("Teaching", "crsCode semester profId", "String String Integer", "crsCode semester");
        Table transcript = new Table("Transcript", "studId crsCode semester grade", "Integer String String String", "studId crsCode semester");
        for (Comparable [] tup : resultTest[0]){
          students.insert(tup);
        }        
        for (Comparable [] tup : resultTest[1]){
          professor.insert(tup);
        }
        for (Comparable [] tup : resultTest[2]){
          course.insert(tup);
        }
        for (Comparable [] tup : resultTest[3]){
          teaching.insert(tup);
        }
        for (Comparable [] tup : resultTest[4]){
          transcript.insert(tup);
        }
        Table[] tableArray = {students, professor, course, teaching, transcript};
        return tableArray;
    }
}
