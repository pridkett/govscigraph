GovSci Graph
============
This is a library of helper functions for manipulation of graphs using
BluePrints from TinkerPop. This abstracts away the constructors and whether
or not a database is transactional. It also automatically adds new elements
to various indices and takes care of formatting properties properly.

Compiling The Code
==================
Maven is used to build the code an manage the project. From the command line
you can run the following command to clean the tree and compile the project
source code:

`mvn clean compile package`

Working with Eclipse
--------------------
If you use Eclipse to develop and run the project, start by installing the
[m2eclipse](http://www.eclipse.org/m2e/) plugin to your Eclipse/RTC
installation. Next, right click on the project in the Package Navigator, and
select "Configure"->"Convert to Maven Project". This should inform Eclipse
that dependencies are managed by Maven and resolve all of your problems.

If that does not resolve your problems you can right click on the project
and select "Maven"->"Update Maven Configuration..." followed by
"Maven"->"Update Dependencies...". This should take care of everything for
you.

Using the Package
=================
In most cases you'll want to create a database management class that extends
BlueprintsBase. And start initialize the graph with something like:
    super("neo4j","/tmp/graph.db", null)
