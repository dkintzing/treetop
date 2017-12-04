this project can be run from the target directory like this. 

java -jar apitest-1.0-SNAPSHOT-jar-with-dependencies.jar
the api will be available on localhost:8000

It was created with IntelliJ so it can be run from within the ide as well.

What I would add if more time.

1.  Quality logging is missing and would be added.
2.  Tests and more testing would be added.
3.  Related to testing, I would look at the code more carefully for robustness.
4.  I used an in memory DB to make it convenient for you to run the code, so depending on 
    the expected load/usage a different DB may be needed.
5.  The web server was also started from the code which would change in a real situation.


