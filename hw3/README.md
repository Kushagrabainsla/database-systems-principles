HW3 - CS257

Files added:
- create.sql  : creates database CS257, schema hw3, tables, constraints, and trigger.
- drop.sql    : drops trigger and tables for cleanup.
- test.sql    : test script that inserts data and demonstrates the trigger behavior.
- test.out    : output file

Notes and assumptions:
- Run scripts with the ^ delimiter: db2 -td"^" -f
- The trigger enforces that for each prereq of a class the student must have at least one prior passing grade meeting the required minimum (A/B/C/D). Grades I or W or NULL are not considered passing. If a prereq is missing, the trigger raises SQLSTATE '75000' with message 'Missing Pre-req'.
- The DDL uses PRIMARY KEYs on student and class to prevent duplicates.
- Foreign keys on class_prereq and schedule have ON DELETE CASCADE to cascade deletes per assignment requirement.
- This does not create the CS257 database. It only performs the DDL/trigger actions (creates tables, constraints, and the classcheck trigger). Create and connect to the database yourself before running the scripts.

Steps to follow:
 - Create database (only once): db2 "create db CS257"
 - Connect to database: db2 "connect to CS257"
 - Run create script: db2 -td"^" -f create.sql
 - Run test script: db2 -td"^" -f test.sql
 - Clean up (drop HW3 objects): db2 -td"^" -f drop.sql
