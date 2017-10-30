# DatabaseProject3

________________________________________________________
Manager Report
________________________________________________________

In this project, we extended the code we wrote in Project 2 
in order to test the efficiency of the different operations
that we implemented with different the data structures.
For select operations, we found that the B+ Tree Map performed
the best out of all other implementations by .45 ms on average
compared to the next fastest implementation, Tree Map.
For range select operations, we found that B+ Tree Map also
performed the best out of all other implementations by
.2 ms on average compared to Tree Map. 
For join operations, we found that Linear Hash Map performed
the best out of all other implementations by .2 ms on average
compared to the next fastest implementation, Tree Map. This
was surprising compared to our other operations, where B+
Tree Map performed the best.


________________________________________________________
To Compile
________________________________________________________

$ javac [.java file name]

or

$ sbt compile (works better with junit test files included)

________________________________________________________
To Run Programs
________________________________________________________

$ java [file name]

or

$ sbt run

________________________________________________________
To Run JUnit Tests
________________________________________________________

$ sbt test


________________________________________________________
Group Member Contributions and Evaluations
________________________________________________________

Kelsie Belan (Project Manager)
Implemented UML Diagram, integration tests, created Github repo

Devin Gray (manager evaluation: +2)
Implemented Join performance tests

Kyle Millar (manager evaluation: +2)
Implemented Point Select performance tests 

Anish Shivkumar (manager evaluation: +2)
Implemented Range Select performance tests

________________________________________________________
Schedule of Meetings
________________________________________________________

10/15/17 (full attendance)

10/23/17 (full attendance)
