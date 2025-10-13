
connect to CS257^

-- Clean tables
delete from hw3.schedule^
delete from hw3.class_prereq^
delete from hw3.class^
delete from hw3.student^

-- Insert data into students table
INSERT INTO hw3.student VALUES
       ('900000','John','Doe','M'),
       ('900001','Jane','Doe','F'),
       ('900002','James','Bond','M'),
       ('900003','Chris','Newman','O'),
       ('900004','Ken','Tsang','M')^

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
       ('300001','CS255','Design & Anal of Alg')^

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
       ('300001','200007','B')^

-- ==========================
-- Test cases for prereq trigger
-- Make sure the database has the trigger installed from create.sql
-- ==========================

-- 1) Missing prereq: John (900000) tries CS46B (100001) without CS46A -> should fail
INSERT INTO hw3.schedule VALUES ('900000','100001',3,2022,NULL)^

-- 2) Satisfy prereq then try again: give John CS46A with a passing grade, then register CS46B
INSERT INTO hw3.schedule VALUES ('900000','100000',1,2021,'B')^
INSERT INTO hw3.schedule VALUES ('900000','100001',3,2022,NULL)^

-- 3) Chained prereq: Jane (900001) tries CS157B (200004) without CS157A -> should fail
INSERT INTO hw3.schedule VALUES ('900001','200004',3,2022,NULL)^

-- 4) Give Jane CS157A with passing grade then register CS157B -> should succeed
INSERT INTO hw3.schedule VALUES ('900001','200001',1,2021,'C')^
INSERT INTO hw3.schedule VALUES ('900001','200004',3,2022,NULL)^

-- 5) Multi-prereq (CS160 = 200003) for James (900002)
--    First try without prereqs -> should fail
INSERT INTO hw3.schedule VALUES ('900002','200003',3,2022,NULL)^
--    Now give all three prereqs (010000,200000,200006) with passing grades then try again -> should succeed
INSERT INTO hw3.schedule VALUES ('900002','010000',1,2020,'C')^
INSERT INTO hw3.schedule VALUES ('900002','200000',2,2020,'B')^
INSERT INTO hw3.schedule VALUES ('900002','200006',3,2020,'C')^
INSERT INTO hw3.schedule VALUES ('900002','200003',3,2022,NULL)^

-- 6) In-progress prereq: Chris (900003) is currently taking 200001 (NULL) then tries 200004 -> should fail
INSERT INTO hw3.schedule VALUES ('900003','200001',2,2022,NULL)^
INSERT INTO hw3.schedule VALUES ('900003','200004',3,2022,NULL)^

-- 7) Repeat class scenario: Ken (900004) failed 100000 then later passed it; afterwards register for 100001 -> should succeed
INSERT INTO hw3.schedule VALUES ('900004','100000',1,2019,'F')^
INSERT INTO hw3.schedule VALUES ('900004','100000',2,2020,'A')^
INSERT INTO hw3.schedule VALUES ('900004','100001',3,2022,NULL)^

-- 8) Partial prereqs: John (900000) has only two of three prereqs for CS160 -> should fail
INSERT INTO hw3.schedule VALUES ('900000','010000',1,2019,'C')^
INSERT INTO hw3.schedule VALUES ('900000','200000',2,2019,'C')^
INSERT INTO hw3.schedule VALUES ('900000','200003',3,2022,NULL)^

-- Show contents to inspect results
select * from hw3.student order by student_id^
select * from hw3.class order by class_id^
select * from hw3.class_prereq order by class_id^
select * from hw3.schedule order by student_id, class_id^

terminate^
