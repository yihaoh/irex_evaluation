SET blmfl.bloomfilter_bitsize TO 64;
SET blmfl.estimated_count TO 10;
SET blmfl.num_hashes TO 15;

-- Set the bitsize
SET blmfl.bloomfilter_bitsize TO 64;
SET blmfl.estimated_count TO 10;
SET blmfl.num_hashes TO 15;

-- Create the extension
CREATE EXTENSION blmfl;

SELECT blmfl_optimal_k(64, 4);

-- Create table Q and blmfl_Q
CREATE TABLE Q(A BIGINT);
CREATE TABLE blmfl_Q(summary BLMFL_RESULT);

-- Create table R and blmfl_R
CREATE TABLE R(A BYTEA, B BYTEA);
CREATE TABLE blmfl_R(summary BLMFL_RESULT);

-- Create table S and blmfl_S
CREATE TABLE S(C BYTEA);
CREATE TABLE blmfl_S(summary BLMFL_RESULT);

-- Create table T and blmfl_T
CREATE TABLE T(D BYTEA);
CREATE TABLE blmfl_T(summary BLMFL_RESULT);

-- Insert values into Q and blmfl_Q (Using blmfl_int for big ints)
INSERT INTO Q VALUES (52), (267), (21474836501), (9223372036854775807), (246812);
INSERT INTO blmfl_Q (SELECT blmfl_int(A) FROM Q);

-- Perform tests to see if certain values are present or not in blmfl_Q
SELECT blmfl_test_int((SELECT summary FROM blmfl_Q), 2);            -- False 
SELECT blmfl_test_int((SELECT summary FROM blmfl_Q), 52);           -- True 
SELECT blmfl_test_int((SELECT summary FROM blmfl_Q), 21474836501);  -- True 
SELECT blmfl_test_int((SELECT summary FROM blmfl_Q), 9223372036854775807);  -- True 

-- Insert values into R and blmfl_R
INSERT INTO R VALUES (numeric_send(253.64), textsend('Hello')), (numeric_send(123.45), textsend('Bye')), (numeric_send(3562.1), textsend('Yay')), (numeric_send(9582.3), textsend('Tests'));
INSERT INTO blmfl_R (SELECT blmfl(A, B) FROM R);
SELECT * FROM blmfl_R;

-- Perform tests to see if certain values are present or not in blmfl_R
SELECT blmfl_test((SELECT summary FROM blmfl_R), numeric_send(253.64), textsend('Invalid'));         -- False 
SELECT blmfl_test((SELECT summary FROM blmfl_R), numeric_send(253.64), textsend('Hello'));           -- True
SELECT blmfl_test((SELECT summary FROM blmfl_R), numeric_send(123.45), textsend('Bye'));             -- True
SELECT blmfl_test((SELECT summary FROM blmfl_R), numeric_send(9582.3), textsend('Tests'));           -- True
SELECT blmfl_test((SELECT summary FROM blmfl_R), numeric_send(123.45), textsend('bye'));             -- False ('B'' should be Uppercase)

SELECT blmfl_fpr((SELECT summary FROM blmfl_R));    -- Need a double parenthesis here

-- Change configurable variable values
SET blmfl.bloomfilter_bitsize TO 350;
SET blmfl.estimated_count TO 5;
SET blmfl.num_hashes TO 23;

-- Insert values into S and blmfl_S
INSERT INTO S VALUES (numeric_send(253464)), (numeric_send(5654635)), (numeric_send(1342534)), (numeric_send(352465)), (numeric_send(473657));
INSERT INTO blmfl_S (SELECT blmfl(C) FROM S);
SELECT * FROM blmfl_S;

-- Perform tests to see if certain values are present or not in blmfl_S
SELECT blmfl_test((SELECT summary FROM blmfl_S), numeric_send(253));         -- False
SELECT blmfl_test((SELECT summary FROM blmfl_S), numeric_send(253464));      -- True
SELECT blmfl_test((SELECT summary FROM blmfl_S), numeric_send(1342534));     -- True
SELECT blmfl_test((SELECT summary FROM blmfl_S), numeric_send(473657));      -- True

SELECT blmfl_fpr((SELECT summary FROM blmfl_S));    -- Need a double parenthesis here

-- Insert values into T and blmfl_T
SET blmfl.estimated_count TO 3;
INSERT INTO T VALUES (numeric_send(1)), (numeric_send(2)), (numeric_send(3));
INSERT INTO blmfl_T (SELECT blmfl(D) FROM T);
SELECT * FROM blmfl_T;

-- Try to merge S and T's bloomfilters
CREATE TABLE blmfl_merged(summary BLMFL_RESULT);
INSERT INTO blmfl_merged (SELECT blmfl_merge((SELECT summary FROM blmfl_S), (SELECT summary FROM blmfl_T)));
SELECT summary FROM blmfl_merged;

SELECT blmfl_test((SELECT summary FROM blmfl_merged), numeric_send(253464));      -- True
SELECT blmfl_test((SELECT summary FROM blmfl_merged), numeric_send(1));           -- True
SELECT blmfl_test((SELECT summary FROM blmfl_merged), numeric_send(100));         -- False

-- Cleanup
DROP TABLE Q;
DROP TABLE R;
DROP TABLE S;
DROP TABLE T;

DROP TABLE blmfl_R;
DROP TABLE blmfl_S;
DROP TABLE blmfl_T;
DROP TABLE blmfl_merged;

DROP EXTENSION blmfl;