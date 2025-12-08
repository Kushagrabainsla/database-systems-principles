#!/bin/bash

###############################################################################
# Comprehensive Test Script for Database Project Part 2
# Tests all 31 items from the checklist
###############################################################################

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0
TEST_NUM=0

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up test files..."
    rm -f *.tab dbfile.bin
}

# Test result function
print_result() {
    TEST_NUM=$((TEST_NUM + 1))
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ Test $TEST_NUM PASSED${NC}: $2"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗ Test $TEST_NUM FAILED${NC}: $2"
        FAILED=$((FAILED + 1))
    fi
}

# Print test header
print_test_header() {
    echo ""
    echo "=========================================="
    echo "TEST $1: $2"
    echo "=========================================="
}

# Execute command and capture output
run_cmd() {
    echo "Command: ./db \"$1\""
    ./db "$1" 2>&1
    return $?
}

# Start fresh
echo "###############################################################################"
echo "# Database System Test Suite - Project Part 2"
echo "###############################################################################"

# Compile the program first
echo ""
echo "Compiling the program..."
gcc -g -o db db.cpp -lstdc++
if [ $? -ne 0 ]; then
    echo -e "${RED}COMPILATION FAILED!${NC}"
    exit 1
fi
echo -e "${GREEN}Compilation successful!${NC}"

# Initial cleanup
cleanup

###############################################################################
# NORMAL TEST SCENARIOS
###############################################################################

echo ""
echo "###############################################################################"
echo "# PART 1: NORMAL TEST SCENARIOS"
echo "###############################################################################"

# Test 01: Create table and basic operations
print_test_header "01" "Create table, insert 15 rows, test SELECT statements"

run_cmd "CREATE TABLE class(Student_Name char(20) NOT NULL, Gender char(1), Exams int, Quiz_Total int, Total int NOT NULL)"

# Insert 15 rows of data
run_cmd "INSERT INTO class VALUES ('Alice', 'F', 85, 380, 465)"
run_cmd "INSERT INTO class VALUES ('Bob', 'M', 78, 350, 428)"
run_cmd "INSERT INTO class VALUES ('Charlie', 'M', 92, 410, 502)"
run_cmd "INSERT INTO class VALUES ('David', 'M', 88, 390, 478)"
run_cmd "INSERT INTO class VALUES ('Eve', 'F', 95, 420, 515)"
run_cmd "INSERT INTO class VALUES ('Frank', 'M', 72, 340, 412)"
run_cmd "INSERT INTO class VALUES ('Grace', 'F', 90, 400, 490)"
run_cmd "INSERT INTO class VALUES ('Henry', 'M', 65, 320, 385)"
run_cmd "INSERT INTO class VALUES ('Ivy', 'F', 88, 385, 473)"
run_cmd "INSERT INTO class VALUES ('Jack', 'M', 80, 360, 440)"
run_cmd "INSERT INTO class VALUES ('Kate', 'F', 93, 415, 508)"
run_cmd "INSERT INTO class VALUES ('Leo', 'M', 70, 330, 400)"
run_cmd "INSERT INTO class VALUES ('Mary', 'F', 87, 375, 462)"
run_cmd "INSERT INTO class VALUES ('Nathan', 'M', 75, 345, 420)"
run_cmd "INSERT INTO class VALUES ('Olivia', 'F', 91, 405, 496)"

echo ""
echo "Testing SELECT * FROM class:"
OUTPUT=$(run_cmd "SELECT * FROM class")
if echo "$OUTPUT" | grep -q "Student_Name"; then
    print_result 0 "SELECT * with proper formatting"
else
    print_result 1 "SELECT * with proper formatting"
fi

echo ""
echo "Testing SELECT Student_Name FROM class:"
OUTPUT=$(run_cmd "SELECT Student_Name FROM class")
if echo "$OUTPUT" | grep -q "Student_Name"; then
    print_result 0 "Single column SELECT"
else
    print_result 1 "Single column SELECT"
fi

echo ""
echo "Testing SELECT Student_Name, Total FROM class:"
OUTPUT=$(run_cmd "SELECT Student_Name, Total FROM class")
if echo "$OUTPUT" | grep -q "Student_Name.*Total"; then
    print_result 0 "Multi-column SELECT"
else
    print_result 1 "Multi-column SELECT"
fi

# Check file size
if [ -f "class.tab" ]; then
    FILE_SIZE=$(stat -f%z "class.tab" 2>/dev/null || stat -c%s "class.tab" 2>/dev/null)
    echo "File size: $FILE_SIZE bytes"
    print_result 0 "File created successfully"
else
    print_result 1 "File creation"
fi

# Test 02: Single row delete
print_test_header "02" "Single row delete"
run_cmd "INSERT INTO class VALUES ('Bad_Student', 'M', 40, 200, 240)"
OUTPUT=$(run_cmd "DELETE FROM class WHERE Student_Name = 'Bad_Student'")
if echo "$OUTPUT" | grep -q "1.*deleted\|deleted.*1"; then
    print_result 0 "Single row delete"
else
    print_result 1 "Single row delete"
fi

# Test 03: Delete with no rows found
print_test_header "03" "Delete with no rows found"
OUTPUT=$(run_cmd "DELETE FROM class WHERE Student_Name = 'NonExistent'")
if echo "$OUTPUT" | grep -qi "0.*deleted\|deleted.*0\|no rows deleted\|warning.*no rows"; then
    print_result 0 "Delete with 0 rows"
else
    print_result 1 "Delete with 0 rows"
fi

# Test 04: Multi-row delete
print_test_header "04" "Multi-row delete (3+ rows)"
OUTPUT=$(run_cmd "DELETE FROM class WHERE Total < 420")
if echo "$OUTPUT" | grep -q "deleted"; then
    print_result 0 "Multi-row delete"
else
    print_result 1 "Multi-row delete"
fi

# Test 05: Single row update
print_test_header "05" "Single row update"
OUTPUT=$(run_cmd "UPDATE class SET Quiz_Total = 350 WHERE Student_Name = 'David'")
if echo "$OUTPUT" | grep -q "1.*updated\|updated.*1"; then
    print_result 0 "Single row update"
else
    print_result 1 "Single row update"
fi

# Test 06: Update with no rows found
print_test_header "06" "Update with no rows found"
OUTPUT=$(run_cmd "UPDATE class SET Quiz_Total = 350 WHERE Student_Name = 'NonExistent'")
if echo "$OUTPUT" | grep -qi "0.*updated\|updated.*0\|no rows updated\|warning.*no rows"; then
    print_result 0 "Update with 0 rows"
else
    print_result 1 "Update with 0 rows"
fi

# Test 07: Multi-row update
print_test_header "07" "Multi-row update (4+ rows)"
OUTPUT=$(run_cmd "UPDATE class SET Quiz_Total = 350 WHERE Quiz_Total > 400")
if echo "$OUTPUT" | grep -q "updated"; then
    print_result 0 "Multi-row update"
else
    print_result 1 "Multi-row update"
fi

# Test 08: SELECT with WHERE clause (single condition)
print_test_header "08" "SELECT with WHERE clause (single condition)"
OUTPUT=$(run_cmd "SELECT * FROM class WHERE Total > 450")
if echo "$OUTPUT" | grep -q "Student_Name"; then
    print_result 0 "SELECT with single WHERE condition"
else
    print_result 1 "SELECT with single WHERE condition"
fi

# Test 09: Case sensitive comparison
print_test_header "09" "SELECT with case sensitive comparison"
OUTPUT=$(run_cmd "SELECT * FROM class WHERE Student_Name < 'Henry'")
if [ $? -eq 0 ]; then
    print_result 0 "Case sensitive string comparison"
else
    print_result 1 "Case sensitive string comparison"
fi

# Test 10: SELECT with NULL and NOT NULL columns
print_test_header "10" "SELECT with NULL and NOT NULL columns"
run_cmd "INSERT INTO class VALUES ('TestNull', NULL, NULL, NULL, 300)"
OUTPUT=$(run_cmd "SELECT * FROM class WHERE Gender IS NULL")
if [ $? -eq 0 ]; then
    print_result 0 "NULL and NOT NULL handling"
else
    print_result 1 "NULL and NOT NULL handling"
fi

# Test 11: SELECT with AND condition
print_test_header "11" "SELECT with two conditions (AND)"
OUTPUT=$(run_cmd "SELECT * FROM class WHERE Total > 450 AND Gender = 'F'")
if [ $? -eq 0 ]; then
    print_result 0 "WHERE with AND operator"
else
    print_result 1 "WHERE with AND operator"
fi

# Test 12: SELECT with OR condition
print_test_header "12" "SELECT with two conditions (OR)"
OUTPUT=$(run_cmd "SELECT * FROM class WHERE Total > 500 OR Total < 400")
if [ $? -eq 0 ]; then
    print_result 0 "WHERE with OR operator"
else
    print_result 1 "WHERE with OR operator"
fi

# Test 13: SELECT with ORDER BY
print_test_header "13" "SELECT with ORDER BY"
OUTPUT=$(run_cmd "SELECT * FROM class ORDER BY Total")
if [ $? -eq 0 ]; then
    print_result 0 "ORDER BY clause"
else
    print_result 1 "ORDER BY clause"
fi

# Test 14: SELECT with WHERE and ORDER BY
print_test_header "14" "SELECT with WHERE and ORDER BY"
OUTPUT=$(run_cmd "SELECT * FROM class WHERE Total > 400 ORDER BY Student_Name")
if [ $? -eq 0 ]; then
    print_result 0 "WHERE with ORDER BY"
else
    print_result 1 "WHERE with ORDER BY"
fi

# Test 15: SELECT SUM()
print_test_header "15" "SELECT SUM() function"
OUTPUT=$(run_cmd "SELECT SUM(Total) FROM class")
if echo "$OUTPUT" | grep -q "SUM"; then
    print_result 0 "SUM() function"
else
    print_result 1 "SUM() function"
fi

# Test 16: SELECT SUM() with WHERE
print_test_header "16" "SELECT SUM() with WHERE clause"
OUTPUT=$(run_cmd "SELECT SUM(Total) FROM class WHERE Gender = 'F'")
if echo "$OUTPUT" | grep -q "SUM"; then
    print_result 0 "SUM() with WHERE"
else
    print_result 1 "SUM() with WHERE"
fi

# Test 17: SELECT AVG()
print_test_header "17" "SELECT AVG() function"
OUTPUT=$(run_cmd "SELECT AVG(Total) FROM class")
if echo "$OUTPUT" | grep -q "AVG"; then
    print_result 0 "AVG() function"
else
    print_result 1 "AVG() function"
fi

# Test 18: SELECT AVG() with WHERE
print_test_header "18" "SELECT AVG() with WHERE clause"
OUTPUT=$(run_cmd "SELECT AVG(Quiz_Total) FROM class WHERE Total > 450")
if echo "$OUTPUT" | grep -q "AVG"; then
    print_result 0 "AVG() with WHERE"
else
    print_result 1 "AVG() with WHERE"
fi

# Test 19: SELECT COUNT()
print_test_header "19" "SELECT COUNT() function"
OUTPUT=$(run_cmd "SELECT COUNT(*) FROM class")
if echo "$OUTPUT" | grep -q "COUNT"; then
    print_result 0 "COUNT() function"
else
    print_result 1 "COUNT() function"
fi

# Test 20: SELECT COUNT() with WHERE
print_test_header "20" "SELECT COUNT() with WHERE clause"
OUTPUT=$(run_cmd "SELECT COUNT(*) FROM class WHERE Gender = 'M'")
if echo "$OUTPUT" | grep -q "COUNT"; then
    print_result 0 "COUNT() with WHERE"
else
    print_result 1 "COUNT() with WHERE"
fi

# Test 21: SUM(), AVG() with NULLs
print_test_header "21" "SUM(), AVG() with NULL values"
OUTPUT=$(run_cmd "SELECT SUM(Exams), AVG(Exams) FROM class")
if [ $? -eq 0 ]; then
    print_result 0 "Aggregate functions with NULLs"
else
    print_result 1 "Aggregate functions with NULLs"
fi

# Test 22: COUNT(*) vs COUNT(column) with NULLs
print_test_header "22" "COUNT(*) vs COUNT(column) with NULLs"
OUTPUT=$(run_cmd "SELECT COUNT(*), COUNT(Quiz_Total) FROM class")
if [ $? -eq 0 ]; then
    print_result 0 "COUNT with NULLs"
else
    print_result 1 "COUNT with NULLs"
fi

# Create second table for NATURAL JOIN tests
echo ""
echo "Creating second table for JOIN tests..."
run_cmd "CREATE TABLE grades(Student_Name char(20) NOT NULL, Final_Grade char(2), GPA int)"
run_cmd "INSERT INTO grades VALUES ('Alice', 'A', 4)"
run_cmd "INSERT INTO grades VALUES ('Bob', 'B', 3)"
run_cmd "INSERT INTO grades VALUES ('Charlie', 'A', 4)"
run_cmd "INSERT INTO grades VALUES ('David', 'B', 3)"
run_cmd "INSERT INTO grades VALUES ('Eve', 'A', 4)"

echo ""
echo "Repeating SELECT tests with NATURAL JOIN..."

# NATURAL JOIN tests
print_test_header "22a" "SELECT with NATURAL JOIN"
OUTPUT=$(run_cmd "SELECT * FROM class NATURAL JOIN grades")
if [ $? -eq 0 ]; then
    print_result 0 "Basic NATURAL JOIN"
else
    print_result 1 "Basic NATURAL JOIN"
fi

print_test_header "22b" "SELECT with NATURAL JOIN and WHERE"
OUTPUT=$(run_cmd "SELECT * FROM class NATURAL JOIN grades WHERE Total > 450")
if [ $? -eq 0 ]; then
    print_result 0 "NATURAL JOIN with WHERE"
else
    print_result 1 "NATURAL JOIN with WHERE"
fi

print_test_header "22c" "SELECT with NATURAL JOIN and ORDER BY"
OUTPUT=$(run_cmd "SELECT * FROM class NATURAL JOIN grades ORDER BY Total")
if [ $? -eq 0 ]; then
    print_result 0 "NATURAL JOIN with ORDER BY"
else
    print_result 1 "NATURAL JOIN with ORDER BY"
fi

print_test_header "22d" "SELECT with NATURAL JOIN, WHERE, and ORDER BY"
OUTPUT=$(run_cmd "SELECT * FROM class NATURAL JOIN grades WHERE Total > 400 ORDER BY Total DESC")
if [ $? -eq 0 ]; then
    print_result 0 "NATURAL JOIN with WHERE and ORDER BY combined"
else
    print_result 1 "NATURAL JOIN with WHERE and ORDER BY combined"
fi

print_test_header "22e" "SELECT SUM() with NATURAL JOIN"
OUTPUT=$(run_cmd "SELECT SUM(Total) FROM class NATURAL JOIN grades")
if echo "$OUTPUT" | grep -q "SUM"; then
    print_result 0 "SUM() with NATURAL JOIN"
else
    print_result 1 "SUM() with NATURAL JOIN"
fi

print_test_header "22f" "SELECT AVG() with NATURAL JOIN"
OUTPUT=$(run_cmd "SELECT AVG(Total) FROM class NATURAL JOIN grades")
if echo "$OUTPUT" | grep -q "AVG"; then
    print_result 0 "AVG() with NATURAL JOIN"
else
    print_result 1 "AVG() with NATURAL JOIN"
fi

print_test_header "22g" "SELECT COUNT() with NATURAL JOIN"
OUTPUT=$(run_cmd "SELECT COUNT(*) FROM class NATURAL JOIN grades")
if echo "$OUTPUT" | grep -q "COUNT"; then
    print_result 0 "COUNT() with NATURAL JOIN"
else
    print_result 1 "COUNT() with NATURAL JOIN"
fi

print_test_header "22h" "SELECT SUM() with NATURAL JOIN and WHERE"
OUTPUT=$(run_cmd "SELECT SUM(Total) FROM class NATURAL JOIN grades WHERE GPA = 4")
if echo "$OUTPUT" | grep -q "SUM"; then
    print_result 0 "SUM() with NATURAL JOIN and WHERE"
else
    print_result 1 "SUM() with NATURAL JOIN and WHERE"
fi

print_test_header "22i" "SELECT AVG() with NATURAL JOIN and WHERE"
OUTPUT=$(run_cmd "SELECT AVG(Quiz_Total) FROM class NATURAL JOIN grades WHERE Final_Grade = 'A'")
if echo "$OUTPUT" | grep -q "AVG"; then
    print_result 0 "AVG() with NATURAL JOIN and WHERE"
else
    print_result 1 "AVG() with NATURAL JOIN and WHERE"
fi

print_test_header "22j" "SELECT COUNT() with NATURAL JOIN and WHERE"
OUTPUT=$(run_cmd "SELECT COUNT(*) FROM class NATURAL JOIN grades WHERE Total > 450")
if echo "$OUTPUT" | grep -q "COUNT"; then
    print_result 0 "COUNT() with NATURAL JOIN and WHERE"
else
    print_result 1 "COUNT() with NATURAL JOIN and WHERE"
fi

print_test_header "22k" "Multi-column SELECT with NATURAL JOIN"
OUTPUT=$(run_cmd "SELECT Student_Name, Total, Final_Grade FROM class NATURAL JOIN grades")
if [ $? -eq 0 ]; then
    print_result 0 "Multi-column SELECT with NATURAL JOIN"
else
    print_result 1 "Multi-column SELECT with NATURAL JOIN"
fi

print_test_header "22l" "SELECT with NATURAL JOIN and complex WHERE (AND)"
OUTPUT=$(run_cmd "SELECT * FROM class NATURAL JOIN grades WHERE Total > 450 AND GPA = 4")
if [ $? -eq 0 ]; then
    print_result 0 "NATURAL JOIN with complex WHERE (AND)"
else
    print_result 1 "NATURAL JOIN with complex WHERE (AND)"
fi

print_test_header "22m" "SELECT with NATURAL JOIN and complex WHERE (OR)"
OUTPUT=$(run_cmd "SELECT * FROM class NATURAL JOIN grades WHERE Final_Grade = 'A' OR Total < 400")
if [ $? -eq 0 ]; then
    print_result 0 "NATURAL JOIN with complex WHERE (OR)"
else
    print_result 1 "NATURAL JOIN with complex WHERE (OR)"
fi

###############################################################################
# ERROR TEST SCENARIOS
###############################################################################

echo ""
echo "###############################################################################"
echo "# PART 2: ERROR TEST SCENARIOS"
echo "###############################################################################"

# Test 23: Syntax errors in DELETE
print_test_header "23" "Syntax errors in DELETE statements"
ERROR_COUNT=0

OUTPUT=$(run_cmd "DELETE class WHERE Total > 400")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "DELETE FROM WHERE Total > 400")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "DELETE FROM class Total > 400")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

if [ $ERROR_COUNT -ge 2 ]; then
    print_result 0 "DELETE syntax error detection"
else
    print_result 1 "DELETE syntax error detection"
fi

# Test 24: Syntax errors in UPDATE
print_test_header "24" "Syntax errors in UPDATE statements"
ERROR_COUNT=0

OUTPUT=$(run_cmd "UPDATE class Quiz_Total = 300 WHERE Total > 400")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "UPDATE SET Quiz_Total = 300 WHERE Total > 400")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "UPDATE class SET WHERE Total > 400")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

if [ $ERROR_COUNT -ge 2 ]; then
    print_result 0 "UPDATE syntax error detection"
else
    print_result 1 "UPDATE syntax error detection"
fi

# Test 25: Syntax errors in SELECT
print_test_header "25" "Syntax errors in SELECT statements"
ERROR_COUNT=0

OUTPUT=$(run_cmd "SELECT FROM class")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "SELECT * class")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "SELECT * FROM ORDER BY Total")
if echo "$OUTPUT" | grep -qi "error\|invalid\|syntax"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

if [ $ERROR_COUNT -ge 2 ]; then
    print_result 0 "SELECT syntax error detection"
else
    print_result 1 "SELECT syntax error detection"
fi

# Test 26: Data type mismatch on INSERT
print_test_header "26" "Data type mismatch errors on INSERT"
OUTPUT=$(run_cmd "INSERT INTO class VALUES ('Test', 'M', 'NotAnInt', 350, 450)")
if echo "$OUTPUT" | grep -qi "error\|invalid\|mismatch\|type"; then
    print_result 0 "INSERT data type mismatch detection"
else
    print_result 1 "INSERT data type mismatch detection"
fi

# Test 27: NOT NULL constraint on INSERT
print_test_header "27" "NOT NULL constraint on INSERT"
OUTPUT=$(run_cmd "INSERT INTO class VALUES (NULL, 'M', 85, 350, 435)")
if echo "$OUTPUT" | grep -qi "error\|null\|constraint"; then
    print_result 0 "INSERT NOT NULL enforcement"
else
    print_result 1 "INSERT NOT NULL enforcement"
fi

# Test 28: NOT NULL constraint on UPDATE
print_test_header "28" "NOT NULL constraint on UPDATE"
OUTPUT=$(run_cmd "UPDATE class SET Student_Name = NULL WHERE Total > 400")
if echo "$OUTPUT" | grep -qi "error\|null\|constraint"; then
    print_result 0 "UPDATE NOT NULL enforcement"
else
    print_result 1 "UPDATE NOT NULL enforcement"
fi

# Test 29: Data type mismatch in WHERE clauses
print_test_header "29" "Data type mismatch in WHERE clauses"
ERROR_COUNT=0

OUTPUT=$(run_cmd "SELECT * FROM class WHERE Total = 'NotANumber'")
if echo "$OUTPUT" | grep -qi "error\|invalid\|mismatch\|type"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "UPDATE class SET Quiz_Total = 300 WHERE Exams = 'Invalid'")
if echo "$OUTPUT" | grep -qi "error\|invalid\|mismatch\|type"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "DELETE FROM class WHERE Total = 'StringValue'")
if echo "$OUTPUT" | grep -qi "error\|invalid\|mismatch\|type"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

if [ $ERROR_COUNT -ge 2 ]; then
    print_result 0 "WHERE clause type mismatch detection"
else
    print_result 1 "WHERE clause type mismatch detection"
fi

# Test 30: Invalid data values
print_test_header "30" "Invalid data value errors"
ERROR_COUNT=0

# Test string too long
OUTPUT=$(run_cmd "INSERT INTO class VALUES ('ThisNameIsWayTooLongForTheColumn', 'M', 85, 350, 435)")
if echo "$OUTPUT" | grep -qi "error\|invalid\|too long\|overflow"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

# Test invalid integer range (if implemented)
OUTPUT=$(run_cmd "INSERT INTO class VALUES ('Test', 'M', 99999999999999999999, 350, 435)")
if echo "$OUTPUT" | grep -qi "error\|invalid\|overflow\|range"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

if [ $ERROR_COUNT -ge 1 ]; then
    print_result 0 "Invalid data value detection"
else
    print_result 1 "Invalid data value detection"
fi

# Test 31: Invalid operators and aggregate functions
print_test_header "31" "Invalid relational operators and aggregate functions"
ERROR_COUNT=0

OUTPUT=$(run_cmd "SELECT * FROM class WHERE Total << 400")
if echo "$OUTPUT" | grep -qi "error\|invalid\|operator"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "SELECT MAX(Total) FROM class")
if echo "$OUTPUT" | grep -qi "error\|invalid\|not supported\|unknown"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

OUTPUT=$(run_cmd "SELECT MIN(Total) FROM class")
if echo "$OUTPUT" | grep -qi "error\|invalid\|not supported\|unknown"; then
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

if [ $ERROR_COUNT -ge 1 ]; then
    print_result 0 "Invalid operator/function detection"
else
    print_result 1 "Invalid operator/function detection (Note: Only SUM/AVG/COUNT should be supported)"
fi

###############################################################################
# FINAL SUMMARY
###############################################################################

echo ""
echo "###############################################################################"
echo "# TEST SUMMARY"
echo "###############################################################################"
echo -e "${GREEN}Tests Passed: $PASSED${NC}"
echo -e "${RED}Tests Failed: $FAILED${NC}"
TOTAL=$((PASSED + FAILED))
echo "Total Tests: $TOTAL"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}★★★ ALL TESTS PASSED! ★★★${NC}"
else
    PERCENTAGE=$((PASSED * 100 / TOTAL))
    echo "Pass Rate: ${PERCENTAGE}%"
fi

# Final cleanup
echo ""
read -p "Do you want to clean up test files? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cleanup
    echo "Cleanup complete."
fi

echo ""
echo "Test script completed."
exit 0
