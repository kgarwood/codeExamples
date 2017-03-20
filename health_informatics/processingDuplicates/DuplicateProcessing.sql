
/*
 * ==============================================================================
 * DE-DUPLICATING HEALTH RECORDS 
 *    by Kevin Garwood
 * ====================================
 *
 * This code example illustrates common techniques for de-duplicating
 * health records.  The basic steps are:
 * (1) Identify the fields you're going to use to tell whether two
 *     records should be considered the same.  
 * (2) Using those fields, assign an identifier to each group of 
 *     duplicates.  In the majority of cases, each group should only
 *     contain one record.  Where a group contains multiple duplicate
 *     records, those records will all have the same identifier.
 * (3) Once you've identified groups of duplicates, you then need to
 *     decide which one to keep in each group.
 * (4) Mark each record with a 'keep' flag field but do not delete
 *     any records.
 * (5) Copy the 'to keep' records into temporary tables and then 
 *     verify that, using the duplicate criteria fields
 *
 * On a more technical level, the key parts of this code example 
 * involve showing the role of database functions such as 
 * row_number() and rank() to help de-duplicate a set of records.
 * 
 * The data set itself is inspired by data sets such as the UK's
 * Health Episode Statistics data set.  But I say 'inspire' because
 * the fields and the fake data set are meant to demonstrate the 
 * concepts behind de-duplication rather than being able to actually
 * process records from a specific data set containing routinely
 * collected health data.
 *
 * I'll leave you to consider a few things.  First, the duplicate with
 * the most populated fields is not necessarily the best one to keep.
 * Consider in the data set how we retain the duplicate that has 
 * three diagnostic codes instead of two.  But what if that extra field
 * is actually a mistake? If the patient is male and the extra diagnostic
 * code is for "ovarian cancer", then we might be better off choosing
 * the record that only has two diagnostic codes but where we know they
 * aren't wrong.  
 *
 * The second issue to think about is to consider whether the time
 * between two successive events describes a duplicate or the same
 * thing happening twice to the same person.  Could someone enter a 
 * hospital because of asthma in two successive epistarts that are
 * a day apart? What about a heart attack?
 *
 * Could the same woman be involved with birth events that are only
 * two months apart?  These questions should make you think about
 * whether applying domain knowledge to de-duplication can help
 * catch more duplicates.
 *
 * ----------------------------------------------------------------------
 * This code has been open sourced under the terms of the GNU Lesser 
 * General Public License as published by the Free Software Foundation, 
 * either version 3 of the License, or (at your option) any later version.
 * The code is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * Please see http://www.gnu.org/licenses.
 */
 
/*
 * =====================================================================
 * Function: load_test_data_set()
 * ------------------------------
 * This function creates a fake data set to support the 
 * demonstration. We only need the minimum number of 
 * records to help demonstrate the purpose of using
 * rank() and row_number() features to help identify and
 * pick duplicates.
 *
 * We don't really care what the diag fields are about.
 * A few are included just to help make it easier to 
 * differentiate duplicate records based on how populated
 * their fields are.  
 * =====================================================================
*/
CREATE OR REPLACE FUNCTION load_test_data_set()
	RETURNS void AS 
$$
DECLARE
	
BEGIN

	DROP TABLE IF EXISTS health_records;
	CREATE TABLE health_records (
	   patient_id TEXT,
	   dob DATE,
	   epistart DATE,
	   epiend DATE,
	   sex TEXT,
	   ethnicity TEXT,
	   diag_01 TEXT,
	   diag_02 TEXT,
	   diag_03 TEXT
	);

	-- Here we're simulating some of the fields you might
	-- find in data sets like HES.  Here, we'll use
	-- these code designations:
	-- sex:
	--    1 = male
	--    2 = female
	--    9 = not specified
	--    0 = Not known
	--
	-- ethnicity:
	-- B = 'Irish (White)'
	-- H = 'Indian (Asian or Asian British)'
	-- ...

	INSERT INTO health_records(
		patient_id,
		dob,
		epistart,
		epiend,
		sex,
		ethnicity,
		diag_01,
		diag_02,
		diag_03) 
	VALUES (
		'123', 
		to_date('13-07-1994', 'DD-MM-YYYY'),
		to_date('21-03-2015', 'DD-MM-YYYY'),
		to_date('24-03-2015', 'DD-MM-YYYY'),
		2,
		'B',
		'code1',
		'code2',
		'code3');

	INSERT INTO health_records(
		patient_id,
		dob,
		epistart,
		epiend,
		sex,
		ethnicity,
		diag_01,
		diag_02,
		diag_03) 
	VALUES (
		'222', 
		to_date('15-06-1993', 'DD-MM-YYYY'),
		to_date('19-04-2015', 'DD-MM-YYYY'),
		to_date('19-04-2015', 'DD-MM-YYYY'),
		1,
		'H',
		'code2',
		null,
		null);


	INSERT INTO health_records(
		patient_id,
		dob,
		epistart,
		epiend,
		sex,
		ethnicity,
		diag_01,
		diag_02,
		diag_03) 
	VALUES (
		'222', 
		to_date('15-06-1993', 'DD-MM-YYYY'),
		to_date('19-04-2015', 'DD-MM-YYYY'),
		to_date('19-04-2015', 'DD-MM-YYYY'),
		1,
		'H',
		'code3',
		null,
		null);


	INSERT INTO health_records(
		patient_id,
		dob,
		epistart,
		epiend,
		sex,
		ethnicity,
		diag_01,
		diag_02,
		diag_03) 
	VALUES (
		'222', 
		to_date('15-06-1993', 'DD-MM-YYYY'),
		to_date('19-04-2015', 'DD-MM-YYYY'),
		to_date('19-04-2015', 'DD-MM-YYYY'),
		1,
		'H',
		'code2',
		'code3',
		null);

	INSERT INTO health_records(
		patient_id,
		dob,
		epistart,
		epiend,
		sex,
		ethnicity,
		diag_01,
		diag_02,
		diag_03) 
	VALUES (
		'222', 
		to_date('15-06-1993', 'DD-MM-YYYY'),
		to_date('19-04-2015', 'DD-MM-YYYY'),
		to_date('19-04-2015', 'DD-MM-YYYY'),
		1,
		'H',
		'code2',
		'code3',
		'code4');

END;

$$   LANGUAGE plpgsql;

/*
 * =====================================================================
 * Step 1: Identify Duplicates
 * ---------------------------
 * In this step, we define the fields we'll use that can tell whether
 * two records can be considered the same.  These fields are:
 * (1) patient_id
 * (2) dob
 * (3) epistart
 * (4) epiend
 *
 * If two records share identical values of all these fields, then 
 * they will be duplicates.  We will use the field 'record_family_id'
 * to assign an identifier to each group of duplicates.  When the
 * records are arranged by patient_id, dob, epistart, and epiend,
 * and whena the combination of these fields changes from one record
 * to the next, a new record_family_id will be assigned.
 * =====================================================================
*/
CREATE OR REPLACE FUNCTION identify_duplicates()
	RETURNS void AS 
$$
DECLARE
	
BEGIN
	DROP TABLE IF EXISTS ranked_health_records;
	CREATE TABLE ranked_health_records AS
	SELECT
		patient_id,
		dob,
		epistart,
		epiend,
		sex,
		ethnicity,
		diag_01,
		diag_02,
		diag_03,
		rank() OVER (ORDER BY 
			patient_id, 
			dob, 
			epistart, 
			epiend) AS record_family_id
	FROM
		health_records;

END;

$$   LANGUAGE plpgsql;


/*
 * =====================================================================
 * Function: get_filled_count
 * --------------------------
 * This function counts the number of fields in a health record
 * which will be populated.  The total number of filled fields
 * can serve as a primitive metric for judging which of one of
 * records within a duplicate group should be kept.
 * =====================================================================
*/
CREATE OR REPLACE FUNCTION get_filled_count(
	patient_id TEXT,
	dob DATE,
	epistart DATE,
	epiend DATE,
	sex TEXT,
	ethnicity TEXT,
	diag_01 TEXT,
	diag_02 TEXT,
	diag_03 TEXT)
	RETURNS TEXT AS 
$$
DECLARE
	filled_count INT;

BEGIN
	
	filled_count := 0;
	
	IF patient_id IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;
	
	IF dob IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;
	
	IF epistart IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;

	IF epiend IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;

	IF sex IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;

	IF ethnicity IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;

	IF diag_01 IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;

	IF diag_02 IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;

	IF diag_03 IS NOT NULL THEN
		filled_count := filled_count + 1;
	END IF;

	RETURN filled_count;
END;

$$   LANGUAGE plpgsql;


/*
 * =====================================================================
 * Step 2: Choose duplicates
 * -------------------------
 * Now that we have assigned identifiers to groups of duplicate records,
 * we have to choose which one to keep for each group.  Here we 
 * explore two ways of picking a record to retain:
 * (1) keep the first duplicate
 * (2) keep the duplicate with the most populated field values
 *
 * The first duplicate refers to first within the order of fields 
 * specified in the OVER part of the row_number() call.  Usually 
 * people will use this approach when they want to just pick an 
 * arbitrary record to keep but the order of the row_number() fields
 * does matter.
 *
 * Another way to choose is to pick the duplicate that has the highest
 * number of filled fields.
 * =====================================================================
*/
CREATE OR REPLACE FUNCTION choose_duplicates_to_keep()
	RETURNS void AS 
$$
DECLARE

BEGIN

	-- In this temporary table we're doing two things:
	-- (1) for each row we're counting the number of filled
	--     fields and
	-- (2) for each row, assign an ith_duplicate number
	DROP TABLE IF EXISTS tmp_choose_duplicates1;
	CREATE TABLE tmp_choose_duplicates1 AS
	SELECT
		patient_id,
		dob,
		epistart,
		epiend,
		sex,
		ethnicity,
		diag_01,
		diag_02,
		diag_03,
		record_family_id,
		get_filled_count(
			patient_id,
			dob,
			epistart,
			epiend,
			sex,
			ethnicity,
			diag_01,
			diag_02,
			diag_03) AS filled_fields,
		row_number() OVER (
			PARTITION BY 
				record_family_id 
			ORDER BY 
				patient_id, 
				dob, 
				epistart, 
				epiend) AS ith_duplicate			
	FROM
		ranked_health_records;

	-- In this table, we will create two 'keep' fields for
	-- identifying duplicate records that should be kept.
	-- 'is_first' and 'is_most_filled' represent choices
	-- based on different criteria.  Notice how we are
	-- NOT deleting any of the duplicate records.  Instead,
	-- we are marking them as deleted.  Marking instead of 
	-- actually deleting duplicate records helps promote
	-- reusability of the final health records data set and
	-- greater repeatability in the studies that depend on it.
	DROP TABLE IF EXISTS health_records_duplicates_flagged;
	CREATE TABLE health_records_duplicates_flagged AS
	WITH max_filled_values AS
		(SELECT
			record_family_id,			
			MAX(filled_fields) AS maximum_filled_value
	 	FROM
		 	tmp_choose_duplicates1
		 GROUP BY 
		 	record_family_id)	
	SELECT
		a.patient_id,
		a.dob,
		a.epistart,
		a.epiend,
		a.sex,
		a.ethnicity,
		a.diag_01,
		a.diag_02,
		a.diag_03,
		a.record_family_id,
		CASE
			WHEN a.ith_duplicate = 1 THEN
				'Y'
			ELSE
				'N'
		END is_first,
		CASE
			WHEN a.filled_fields = b.maximum_filled_value THEN
				'Y'
			ELSE
				'N'
		END is_most_filled
	FROM
		tmp_choose_duplicates1 a,
		max_filled_values b
	WHERE
		a.record_family_id = b.record_family_id
	ORDER BY
		patient_id,
		dob,
		epistart,
		epiend;

END;

$$   LANGUAGE plpgsql;

/*
 * =====================================================================
 * Function: verify_deduplicated_records()
 * --------------------------------------- 
 * We verify that regardless of the approach we've taken to choose
 * which duplicate records to keep, they should be able to produce a
 * data set that in fact has eliminated duplicates.
 *
 * Although we do not delete duplicate records, here we try to 
 * create temporary tables to demonstrate that the the kept duplicate
 * values will produce a table with unique rows.
 *
 * The approach relies on trying to add a primary key to tables
 * that use each 'keep' field as a filter. If the filter fails, then
 * the approach for de-duplication has failed.
 *
 * =====================================================================
*/
CREATE OR REPLACE FUNCTION verify_deduplication_approaches()
	RETURNS void AS 
$$
DECLARE
	

BEGIN

	-- Create a temporary table that keeps duplicates based
	-- on whether they have the most populated fields in 
	-- their group of duplicates.
	DROP TABLE IF EXISTS validate_first_duplicate_approach;
	CREATE TABLE validate_first_duplicate_approach AS
	SELECT
		*
	FROM
		health_records_duplicates_flagged
	WHERE
		is_most_filled = 'Y';

	ALTER TABLE validate_first_duplicate_approach ADD PRIMARY KEY(
		patient_id, 
		dob, 
		epistart, 
		epiend);

	-- Create another temporary table that keeps duplicates based
	-- on whether the duplicate appears first in an ordering of
	-- records within a duplicate group.
	DROP TABLE IF EXISTS validate_most_filled_approach;
	CREATE TABLE validate_most_filled_approach AS
	SELECT
		*
	FROM
		health_records_duplicates_flagged
	WHERE
		is_first = 'Y';

	ALTER TABLE validate_most_filled_approach ADD PRIMARY KEY(
		patient_id, 
		dob, 
		epistart, 
		epiend);
END;

$$   LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION process_duplicates()
	RETURNS void AS 
$$
DECLARE
	
BEGIN

	PERFORM load_test_data_set();
	
	-- As an example of exploring data variance, uncomment this
	-- line to observe where duplicates are happening in the 
	-- patient records data set.  Note, this will cause the program
	-- to fail.
	-- ALTER TABLE health_records ADD PRIMARY KEY(patient_id, dob, epistart, epiend);
	
	PERFORM identify_duplicates();
	PERFORM choose_duplicates_to_keep();	
	PERFORM verify_deduplication_approaches();
END;
$$   LANGUAGE plpgsql;

-- Main example
SELECT "process_duplicates"()


