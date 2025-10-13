-- hw3.sql
-- Test script to exercise the trigger and constraints
-- Run with: db2 -td"^" -f hw3.sql

connect to CS257^
-- Clean tables
delete from hw3.schedule^
delete from hw3.class_prereq^
delete from hw3.class^
delete from hw3.student^

-- Insert sample students
INSERT INTO hw3.student VALUES
       ('900000','John','Doe','M'),
       ('900001','Jane','Doe','F')^

-- Insert sample classes
INSERT INTO hw3.class VALUES
       ('100000','CS46A','Intro to Programming'),
       ('100001','CS46B','Intro to Data Struct'),
       ('200001','CS157A','Intro to DBMS'),
       ('200004','CS157B','DBMS II')^

-- Insert prereqs: CS46B requires CS46A (C), CS157B requires CS157A (C)
INSERT INTO hw3.class_prereq VALUES
       ('100001','100000','C'),
       ('200004','200001','C')^

-- 1) Attempt to register John (900000) for CS46B without having CS46A -> should fail
-- This should produce 'Missing Pre-req' error from trigger
INSERT INTO hw3.schedule VALUES ('900000','100001',3,2022,NULL)^

-- 2) Give John a passing grade in CS46A, then try again
INSERT INTO hw3.schedule VALUES ('900000','100000',1,2021,'B')^
INSERT INTO hw3.schedule VALUES ('900000','100001',3,2022,NULL)^

-- 3) Test chained prereq: try to insert CS157B for Jane without CS157A -> fail
INSERT INTO hw3.schedule VALUES ('900001','200004',3,2022,NULL)^

-- 4) Insert CS157A for Jane with grade C, then try CS157B again (should succeed)
INSERT INTO hw3.schedule VALUES ('900001','200001',1,2021,'C')^
INSERT INTO hw3.schedule VALUES ('900001','200004',3,2022,NULL)^

-- Show contents
select * from hw3.student order by student_id^
select * from hw3.class order by class_id^
select * from hw3.class_prereq order by class_id^
select * from hw3.schedule order by student_id, class_id^

terminate^
