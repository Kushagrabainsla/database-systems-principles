
connect to CS257^

-- Student table: disallow duplicate student_id via PRIMARY KEY
CREATE TABLE hw3.student (
       student_id CHAR(6) NOT NULL,
       first VARCHAR(15) NOT NULL,
       last VARCHAR(15) NOT NULL,
       gender CHAR(1) NOT NULL CHECK (gender IN ('M','F','O')),
       PRIMARY KEY (student_id)
)^

-- Class table: disallow duplicate class_id via PRIMARY KEY
CREATE TABLE hw3.class (
       class_id CHAR(6) NOT NULL,
       name VARCHAR(20) NOT NULL,
       desc VARCHAR(20) NOT NULL,
       PRIMARY KEY (class_id)
)^

-- Class prerequisites: both class_id and prereq_id must exist in hw3.class
-- Cascade deletes so that if a class is dropped its prereq rows are removed
CREATE TABLE hw3.class_prereq (
       class_id CHAR(6) NOT NULL,
       prereq_id CHAR(6) NOT NULL,
       req_grade CHAR(1) NOT NULL CHECK (req_grade IN ('A','B','C','D')),
       PRIMARY KEY (class_id, prereq_id),
       FOREIGN KEY (class_id) REFERENCES hw3.class(class_id) ON DELETE CASCADE,
       FOREIGN KEY (prereq_id) REFERENCES hw3.class(class_id) ON DELETE CASCADE,
       CHECK (prereq_id <> class_id)
)^

-- Schedule table: foreign keys enforce valid student_id and class_id
-- ON DELETE CASCADE for class means deleting a class removes schedule entries
CREATE TABLE hw3.schedule (
       student_id CHAR(6) NOT NULL,
       class_id CHAR(6) NOT NULL,
       semester INT NOT NULL CHECK (semester IN (1,2,3)),
       year INT NOT NULL CHECK (year >= 1950 AND year <= 2022),
       grade CHAR(1) CHECK (grade IN ('A','B','C','D','F','I','W') OR grade IS NULL),
       PRIMARY KEY (student_id, class_id, semester, year),
       FOREIGN KEY (student_id) REFERENCES hw3.student(student_id) ON DELETE CASCADE,
       FOREIGN KEY (class_id) REFERENCES hw3.class(class_id) ON DELETE CASCADE
)^

-- Trigger to enforce pre-req completion before inserting into schedule
-- Assumptions enforced by trigger:
--  * Single-row inserts into hw3.schedule
--  * If any prereq is missing (no passing grade), signal error "Missing Pre-req"
CREATE TRIGGER hw3.classcheck
NO CASCADE BEFORE INSERT ON hw3.schedule
REFERENCING NEW AS N
FOR EACH ROW MODE DB2SQL
BEGIN ATOMIC
    DECLARE V_MISSING INT DEFAULT 0;
    DECLARE V_INPROGRESS INT DEFAULT 0;

    -- 1) Any pre-req being taken with no grade yet? -> reject
    SET V_INPROGRESS = (
        SELECT COUNT(*)
        FROM hw3.class_prereq P
        JOIN hw3.schedule S
            ON S.class_id = P.prereq_id AND S.student_id = N.student_id
        WHERE P.class_id = N.class_id
        AND S.grade IS NULL
    );

    -- 2) For each pre-req, ensure at least ONE prior passing grade meeting threshold
    --    Threshold logic by set containment to avoid letter math.
    SET V_MISSING = (
        SELECT COUNT(*)
        FROM HW3.class_prereq P
        WHERE P.class_id = N.class_id
        AND NOT EXISTS (
            SELECT 1
            FROM HW3.schedule S
            WHERE S.student_id = N.student_id
                AND S.class_id = P.prereq_id
                AND S.grade IS NOT NULL
                AND (
                    (P.req_grade = 'A' AND S.grade IN ('A')) OR
                    (P.req_grade = 'B' AND S.grade IN ('A','B')) OR
                    (P.req_grade = 'C' AND S.grade IN ('A','B','C')) OR
                    (P.req_grade = 'D' AND S.grade IN ('A','B','C','D'))
                )
        )
    );

    IF V_INPROGRESS > 0 OR V_MISSING > 0 THEN
    SIGNAL SQLSTATE '75001'
        SET MESSAGE_TEXT = 'Missing Pre-req';
    END IF;
END^


terminate^
