-- Test.sql for HW#3
connect to cs257;
delete from hw3.student;
delete from hw3.class;
delete from hw3.class_prereq;
delete from hw3.schedule;

--Inserting data into the student table
INSERT INTO hw3.student VALUES
       ('900000','John','Doe','M'),
       ('900001','Jane','Doe','F'),
       ('900002','James','Bond','M'),
       ('900003','Chris','Newman','O'),
       ('900004','Ken','Tsang','M');

--Inserting data into the class table
INSERT INTO hw3.class VALUES
       ('010000','CS100W','Technical Writing'),
       ('100000','CS46A','Intro to Programming'),
       ('100001','CS46B','Intro to Data Struct'),
       ('100002','CS47', 'Intro to Comp Sys'),
       ('100003','CS49J','Programming in Java'),
       ('200000','CS146','Data Structure & Alg'),
       ('200001','CS157A','Intro to DBMS'),
       ('200002','CS149','Operating Systems'),
       ('200003','CS160','Software Engineering'),
       ('200004','CS157B','DBMS II'),
       ('200005','CS157C','NoSQL DB Systems'),
       ('200006','CS151','OO Design'),
       ('200007','CS155','Design & Anal of Alg'),
       ('300000','CS257','DB Mgmt Principles'),
       ('300001','CS255','Design & Anal of Alg');

--Inserting data into the classreq table
INSERT INTO hw3.class_prereq VALUES
       ('100001','100000','C'),
       ('100002','100001','C'),
       ('200000','100001','C'),
       ('200001','200000','C'),
       ('200002','200000','C'),
       ('200003','010000','C'),
       ('200003','200000','C'),
       ('200003','200006','C'),
       ('200004','200001','C'),
       ('200005','200001','C'),
       ('200006','100001','C'),
       ('200007','200000','B'),
       ('300000','200004','B'),
       ('300001','200007','B');
 
  
--Test #1 (HW3.STUDENT)
INSERT INTO hw3.student VALUES ('000001','Test','Student 1','M');
INSERT INTO hw3.student VALUES ('000002','Test','Student 2','F');
INSERT INTO hw3.student VALUES ('000003','Test','Student 3','O');
-- error tests
INSERT INTO hw3.student VALUES ('000001','Duplicate','ID test','F');
INSERT INTO hw3.student VALUES (NULL, 'Test','Student 2','M');
INSERT INTO hw3.student VALUES ('000004', NULL,'Student 3','M');
INSERT INTO hw3.student VALUES ('000004', 'Test','Student 4','f');
INSERT INTO hw3.student VALUES ('000005', 'Test','Student 5','Z');
INSERT INTO hw3.student VALUES ('000006', 'Test','Student 6',NULL);

--Test #2 (HW3.CLASS)
INSERT INTO hw3.class VALUES ('000010','CS42','Discrete Mathematics');
INSERT INTO hw3.class VALUES ('000020','CS185C','Special Topics');
-- error tests
INSERT INTO hw3.class VALUES ('000010','Duplicate','ID test');
INSERT INTO hw3.class VALUES (NULL,'CS42','BadID Test');
INSERT INTO hw3.class VALUES ('000011',NULL,'NULL Test');
INSERT INTO hw3.class VALUES ('000012','CS25',NULL);

--Test #3 (HW3.CLASS_PREREQ) Error tests
INSERT INTO hw3.class_prereq VALUES ('000010','999999','D');
INSERT INTO hw3.class_prereq VALUES ('000010',NULL,'C');
INSERT INTO hw3.class_prereq VALUES ('999999','000010','F');

--Test #4 (HW3.SCHEDULE) non-existing class id and student id (Foregin Key constraint)
INSERT INTO hw3.schedule VALUES ('999999','100000',1,2019,'A');
INSERT INTO hw3.schedule VALUES ('000001','999999',2,2019,'B');
--Test invalid values which should be block by CHECK contraints, invalid semester & grades
INSERT INTO hw3.schedule VALUES ('000001','000020',4,2019,'C');
INSERT INTO hw3.schedule VALUES ('000002','000020',3,2019,'E');
INSERT INTO hw3.schedule VALUES ('000003','000020',3,2019,'a');

--Test cascade drop of HW3.CLASS (Question 2 from spec), the 2 (SELECT * ...) stmts after DELETE should have 0 record
INSERT INTO hw3.schedule VALUES ('000001','000020',1,2019,'A');
INSERT INTO hw3.class_prereq VALUES ('000020','000010','C');
SELECT * FROM hw3.class_prereq WHERE hw3.class_prereq.class_id = '000020';
SELECT * FROM hw3.schedule WHERE hw3.schedule.class_id = '000020';
DELETE FROM hw3.class WHERE hw3.class.class_id = '000020';
SELECT * FROM hw3.class_prereq WHERE hw3.class_prereq.class_id = '000020';
SELECT * FROM hw3.schedule WHERE hw3.schedule.class_id = '000020';

--Test #5 Test Trigger, first empty the table just in case
DELETE from hw3.schedule;
-- Start with student '900000'
-- Add CS100W, no prereq, should work!
INSERT INTO hw3.schedule VALUES ('900000','010000',1,2018,'C');
-- Add CS46B but no CS46A yet, should fail
INSERT INTO hw3.schedule VALUES ('900000','100001',2,2018,NULL); 
-- Add CS46A without grade, should work!
INSERT INTO hw3.schedule VALUES ('900000','100000',1,2018,NULL); 
-- Add CS46B but no CS46A grade, should fail
INSERT INTO hw3.schedule VALUES ('900000','100001',1,2018,NULL); 
-- Update CS46A grade to 'F'
UPDATE hw3.schedule SET hw3.schedule.grade = 'F' where hw3.schedule.student_id = '900000' AND hw3.schedule.class_id = '100000'; 
-- Add CS46B but CS47A grade is 'F', should fail 
INSERT INTO hw3.schedule VALUES ('900000','100001',1,2018,NULL); 
-- Update CS46A grade to 'B'
UPDATE hw3.schedule SET hw3.schedule.grade = 'B' where hw3.schedule.student_id = '900000' AND hw3.schedule.class_id = '100000';  
-- Add CS46B with CS46A grade = 'B', should work!
INSERT INTO hw3.schedule VALUES ('900000','100001',2,2018,'A'); 
-- Add CS151 with prereq CS46B = 'B', should work!
INSERT INTO hw3.schedule VALUES ('900000','200006',3,2018,'B'); 
-- Add CS160, has CS151,CS100W, but no CS146, should FAIL
INSERT INTO hw3.schedule VALUES ('900000','200003',1,2019,'B');
-- Add CS157A but missing prereq, should fail
INSERT INTO hw3.schedule VALUES ('900000','200001',1,2019,'B');
-- Add CS146 with prereq CS46B = 'B', should work!
INSERT INTO hw3.schedule VALUES ('900000','200000',3,2018,'A'); 
-- Add CS157A with prereq CS146 = 'A', should work!
INSERT INTO hw3.schedule VALUES ('900000','200001',1,2019,'B');
-- Add CS160, has CS151,CS100W,and CS146, should work!!!
INSERT INTO hw3.schedule VALUES ('900000','200003',1,2019,'B');
-- Finished with student '900000'
-- Test with student '900001'
-- Add CS100W, no prereq, should work!
INSERT INTO hw3.schedule VALUES ('900001','010000',1,2018,'A'); 
-- Add CS46B but no CS46A yet, should fail
INSERT INTO hw3.schedule VALUES ('900001','100001',2,2018,'B');
-- Add CS157A but missing prereq, should fail
INSERT INTO hw3.schedule VALUES ('900001','200001',1,2019,'B');
-- Add CS46A, no prereq, should work!
INSERT INTO hw3.schedule VALUES ('900001','100000',1,2018,'A'); 
-- Add CS46B with CS46A grade = 'A', should work!
INSERT INTO hw3.schedule VALUES ('900001','100001',2,2018,'A'); 
-- Add CS160, has CS100W, but no CS146,CS151, should FAIL
INSERT INTO hw3.schedule VALUES ('900001','200003',1,2019,'B'); 
-- Add CS151 with prereq CS46B = 'B', should work!
INSERT INTO hw3.schedule VALUES ('900001','200006',3,2018,'B'); 
-- Add CS146 with prereq CS46B = 'B', should work!
INSERT INTO hw3.schedule VALUES ('900001','200000',3,2018,'F'); 
-- Add CS160, has CS151,CS100W,but CS146='F', should FAIL
INSERT INTO hw3.schedule VALUES ('900001','200003',1,2019,'B'); 
-- Update CS146 grade to 'B'
UPDATE hw3.schedule SET hw3.schedule.grade = 'B' where hw3.schedule.student_id = '900001' AND hw3.schedule.class_id = '200000';  
-- Add CS160, has CS151,CS100W,and CS146, should work!!!
INSERT INTO hw3.schedule VALUES ('900001','200003',1,2019,'B'); 
-- Finished with student '9000'
-- Test with student '9002'
-- Add CS46A, no prereq, should work!
INSERT INTO hw3.schedule VALUES ('900002','100000',1,2020,'D'); 
-- Add CS46A again in a later semester, no prereq, should work!
INSERT INTO hw3.schedule VALUES ('900002','100000',1,2021,'C'); 
-- Add CS46A again in a later semester, no prereq, should work!
INSERT INTO hw3.schedule VALUES ('900002','100000',2,2021,'A'); 
-- Add CS46B with more than 1 CS46A grades (highest is 'A') should work!
INSERT INTO hw3.schedule VALUES ('900002','100001',3,2021,'B');
-- Add CS146 with prereq CS46B = 'B', should work!
-- Finished with student '9002'
-- Test with student '9003'
-- Add CS46A, no prereq, should work!
INSERT INTO hw3.schedule VALUES ('900003','100000',1,2020,'B'); 
-- Add CS46B with CS46A grade = 'A', should work!
INSERT INTO hw3.schedule VALUES ('900003','100001',2,2020,'A'); 
-- Add CS146 with prereq CS46B = 'B', should work!
INSERT INTO hw3.schedule VALUES ('900003','200000',3,2020,'C'); 
-- Add CS155 with prereq CS146B = 'C', req a 'B' should fail!
INSERT INTO hw3.schedule VALUES ('900003','200007',1,2021,'C'); 
-- Update CS146 grade to 'B'
UPDATE hw3.schedule SET hw3.schedule.grade = 'B' where hw3.schedule.student_id = '900003' AND hw3.schedule.class_id = '200000';  
-- Add CS155 with prereq CS146B = 'B', req a 'B' should work!
INSERT INTO hw3.schedule VALUES ('900003','200007',1,2021,'C'); 
-- Add CS255 with prereq CS155 = 'C', req a 'B' should fail!
INSERT INTO hw3.schedule VALUES ('900003','300001',2,2021,'A'); 
-- Update CS155 grade to 'A'
UPDATE hw3.schedule SET hw3.schedule.grade = 'A' where hw3.schedule.student_id = '900003' AND hw3.schedule.class_id = '200007';  
-- Add CS255 with prereq CS155 = 'A', req a 'B' should work!
INSERT INTO hw3.schedule VALUES ('900003','300001',2,2021,'A'); 
-- Finished with student '9003'
-- The End

-- Check table contents after all the tests
select * from hw3.student order by student_id;
select * from hw3.class order by class_id;
select * from hw3.class_prereq order by class_id;
select * from hw3.schedule order by student_id, class_id;

terminate;