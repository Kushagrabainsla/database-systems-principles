-- HW#3
-- Section x
-- Student ID: xxxxxxxxx
-- Last Name, First Name
-- In order to run this sql script with the ^ as your line delimiter, use the follow command options
-- db2 -td"^" -f hw3_sample.sql

connect to cs257^
drop TRIGGER hw3.classcheck^
drop table hw3.schedule^
drop table hw3.class_prereq^
drop table hw3.class^
drop table hw3.student^

--Creating student table
CREATE TABLE hw3.student (
       student_id char (6),
       first varchar (15),
       last varchar (15),
       gender char (1)
)^

--Creating class table
CREATE TABLE hw3.class (
       class_id char (6),
       name varchar (20),
       desc varchar (20)
)^

--Creating pre-req table
CREATE TABLE hw3.class_prereq (
       class_id char (6),
       prereq_id char (6),
       req_grade char (1)
)^

--Creating schedule table
CREATE TABLE hw3.schedule (
       student_id char (6),
       class_id char (6),
       semester int,
       year int,
       grade char (1)
)^

--Creating trigger for pre-req
CREATE TRIGGER hw3.classcheck
NO CASCADE BEFORE INSERT ON hw3.schedule
REFERENCING NEW AS newrow  
FOR EACH ROW MODE DB2SQL
WHEN ( 0 < (SELECT COUNT(*)
              FROM hw3.class_prereq 
              WHERE hw3.class_prereq.class_id = newrow.class_id ) ) 
BEGIN ATOMIC
       DECLARE num_prereq int;
       DECLARE prereq_pass int;

       SET num_prereq = (SELECT COUNT(*)
                            FROM hw3.class_prereq 
                            WHERE hw3.class_prereq.class_id = newrow.class_id);

       IF ( num_prereq > 0 ) 
       THEN      SIGNAL SQLSTATE '88888' ( 'Missing Pre-req' );
       END IF;
END^
