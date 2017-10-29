public class PerformanceTest {

	public static void main(String[] args) {
		Table[] tableArray = fillTables();

    //-----------select tests---------------

    System.out.println("---------Select---------");
    long start = System.nanoTime();
    //Sequential Select status
    Table selectTable = tableArray[0].select(t -> t[tableArray[0].col("status")].equals("930409"));
    long end = System.nanoTime();
    double duration = (end - start)/1000000.0;
    System.out.println("Sequential Select\nTime: " +duration + " ms");

    for(int i=0; i<3; i++){
      start=System.nanoTime();
      //Index Select status
      selectTable = tableArray[0].select(new KeyType(930409), i);
      end = System.nanoTime();
      duration = (end - start)/1000000.0;
      switch(i){
      case 0:
        System.out.println("LinHashMap");
        break;
      case 1:
        System.out.println("TreeMap");
        break;
      case 2:
        System.out.println("BPlusTreeMap");
        break;        
      }
      System.out.println("Time: " + duration + " ms\n");
    }
		

    //-----------Range select tests---------------

    System.out.println("---------Range Select---------");		
	for (int k = 0;k <= 1;k++)
	{
		Table testValues;
		timeStart = System.nanoTime();
		if (k == 1)
		{
			tableArray[0].bpIndex.subMap(new KeyType(930390), new KeyType(930409)).entrySet().forEach(e -> e.getValue());
		}
		else {
			testValues = tableArray[0].select(t -> t[0].compareTo(930390) >= 0 && t[0].compareTo(930409) <= 0, k);
		}
		timeEnd = System.nanoTime();
		finalTime = (timeEnd - timeStart) / 1000000.0;
		switch(k)
		{
			case 0:
				System.out.println("TreeMap");
				break;
			case 1:
				System.out.println("BPTreeMap");
				break;
			default:
				break;
		}
		System.out.println("Time - " + finalTime + " ms");
	}

    //-----------join tests-----------------

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
		
		for(int i=0; i<3; i++){
			start=System.nanoTime();
			//Index Join Student to Transcript
			joinTable = tableArray[4].i_join("studId", "id", tableArray[0],i);
			end = System.nanoTime();
			duration = (end - start)/1000000.0;
			switch(i){
			case 0:
				System.out.println("TreeMap");
				break;
			case 1:
				System.out.println("LinHashMap");
				break;
			case 2:
				System.out.println("BPlusTreeMap");
				break;				
			}
			System.out.println("Time: " + duration + " ms");
		}
	}

	public static Table[] fillTables(){
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

        
        int tups [] = new int [] { 30, 15, 10, 25, 18 };
    
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
