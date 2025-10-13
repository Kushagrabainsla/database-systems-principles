HW3 - CS257

Files added:
- create.sql  : creates database CS257, schema hw3, tables, constraints, and trigger.
- drop.sql    : drops trigger and tables for cleanup.
- hw3.sql     : test script that inserts data and demonstrates the trigger behavior.

Notes and assumptions:
- Run scripts with the ^ delimiter: db2 -td"^" -f <script>
- The trigger enforces that for each prereq of a class the student must have at least one prior passing grade meeting the required minimum (A/B/C/D). Grades I or W or NULL are not considered passing. If a prereq is missing, the trigger raises SQLSTATE '75000' with message 'Missing Pre-req'.
- The DDL uses PRIMARY KEYs on student and class to prevent duplicates.
- Foreign keys on class_prereq and schedule have ON DELETE CASCADE to cascade deletes per assignment requirement.
- The create script issues CREATE DB CS257. If the DB already exists, that statement will fail; in that case manually connect to CS257 and run the remainder of the script or remove that line.

If you'd like, I can update the scripts to avoid creating the DB (useful if you already have CS257) or add more test cases.
