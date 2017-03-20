





/*
 * ===============================================================================
 * FUNCTION: load_sample_maternity_data
 * ------------------------------------
 * Loads sample maternity episode records that are based on England's 
 * Hospital Episode Statistics data set.  The table contains a small number of 
 * fake records to demonstrate some of the data quality checks that could be used
 * to score HES records.
 *
 * Here, we also do some basic pre-processing.  For those fields which may 
 * initially contain text values, we ensure that NULL is used either when NULL
 * actually appears or when the field is actually blank with just spaces. 
 *
 * ===============================================================================
 */
CREATE OR REPLACE FUNCTION load_sample_maternity_data()
	RETURNS VOID AS 
$$
DECLARE
	
BEGIN

	DROP TABLE IF EXISTS tmp_maternity_episode_data;
	CREATE TABLE tmp_maternity_episode_data (
		year INT,
		extract_hes_id TEXT,
		description TEXT,
		dob DATE,
		ethnos TEXT,	
		admidate DATE,	
		procode TEXT,
		sex TEXT,
		epitype TEXT,
		epistart DATE,
		epiend DATE,
		epiorder TEXT,
		matage TEXT,	
		numbaby TEXT,
		dobbaby1 DATE,
		biresus1 TEXT,
		delstat1 TEXT,
		birorder1 TEXT,
		birstat1 TEXT,
		birweit1 TEXT,
		delmeth1 TEXT,
		delplace1 TEXT,
		gestat1 TEXT,
		sexbaby1 TEXT,
		dobbaby2 DATE,
		biresus2 TEXT,
		delstat2 TEXT,
		birorder2 TEXT,
		birstat2 TEXT,
		birweit2 TEXT,
		delmeth2 TEXT,
		delplace2 TEXT,
		gestat2 TEXT,
		sexbaby2 TEXT,
		dobbaby3 DATE,
		biresus3 TEXT,
		delstat3 TEXT,
		birorder3 TEXT,
		birstat3 TEXT,
		birweit3 TEXT,
		delmeth3 TEXT,
		delplace3 TEXT,
		gestat3 TEXT,
		sexbaby3 TEXT);

	EXECUTE format ('
	COPY tmp_maternity_episode_data (	
		year,
		extract_hes_id,
		description,
		dob,
		ethnos,	
		admidate,	
		procode,
		sex,
		epitype,
		epistart,
		epiend,
		epiorder,
		matage,	
		numbaby,
		dobbaby1,
		biresus1,
		delstat1,
		birorder1,
		birstat1,
		birweit1,
		delmeth1,
		delplace1,
		gestat1,
		sexbaby1,
		dobbaby2,
		biresus2,
		delstat2,
		birorder2,
		birstat2,
		birweit2,
		delmeth2,
		delplace2,
		gestat2,
		sexbaby2,
		dobbaby3,
		biresus3,
		delstat3,
		birorder3,
		birstat3,
		birweit3,
		delmeth3,
		delplace3,
		gestat3,
		sexbaby3)
	FROM 
		%L
	(FORMAT CSV, HEADER)', 'C:/test_icd/maternity_hes.csv');








	DROP TABLE IF EXISTS original_maternity_episode_data;
	CREATE TABLE original_maternity_episode_data AS
	SELECT
		year,
		row_number() OVER (PARTITION BY year) AS file_row,			
		NULLIF(TRIM(description), '') AS description,
		NULLIF(TRIM(extract_hes_id), '') AS extract_hes_id, 
		NULLIF(TRIM(procode), '') AS procode,
		epistart,
		epiend,
		CASE
			WHEN TRIM(epiorder) = '' THEN '98'
			WHEN epiorder IS NULL THEN '98'
			ELSE epiorder
		END AS epiorder,
		dob,
		NULLIF(TRIM(ethnos), '') AS ethnos,
		admidate,
		CASE
			WHEN TRIM(sex) = '' THEN NULL
			WHEN sex = 'M' THEN '1'
			WHEN sex = 'F' THEN '2'
			WHEN sex = 'U' THEN '0'
			ELSE sex
		END AS sex,
		NULLIF(TRIM(epitype), '') AS epitype,
		NULLIF(TRIM(matage), '') AS matage,
		NULLIF(TRIM(numbaby), '') AS numbaby,
		dobbaby1,
		NULLIF(TRIM(biresus1), '') AS biresus1,
		NULLIF(TRIM(delstat1), '') AS delstat1,
		CASE 
			WHEN TRIM(birorder1) = '' THEN NULL
			WHEN birorder1 IS NULL THEN NULL
			WHEN birorder1 = 'X' THEN '9'
			ELSE birorder1
		END AS birorder1,
		NULLIF(TRIM(birstat1), '') AS birstat1,
		NULLIF(TRIM(birweit1), '') AS birweit1,
		NULLIF(TRIM(delmeth1), '') AS delmeth1,
		NULLIF(TRIM(delplace1), '') AS delplace1,
		NULLIF(TRIM(gestat1), '') AS gestat1,
		NULLIF(TRIM(sexbaby1), '') AS sexbaby1,
		dobbaby2,
		NULLIF(TRIM(biresus2), '') AS biresus2,
		NULLIF(TRIM(delstat2), '') AS delstat2,
		CASE 
			WHEN TRIM(birorder2) = '' THEN NULL
			WHEN birorder2 IS NULL THEN NULL
			WHEN birorder2 = 'X' THEN '9'
			ELSE birorder2
		END AS birorder2,
		NULLIF(TRIM(birstat2), '') AS birstat2,
		NULLIF(TRIM(birweit2), '') AS birweit2,
		NULLIF(TRIM(delmeth2), '') AS delmeth2,
		NULLIF(TRIM(delplace2), '') AS delplace2,
		NULLIF(TRIM(gestat2), '') AS gestat2,
		NULLIF(TRIM(sexbaby2), '') AS sexbaby2,
		dobbaby3,
		NULLIF(TRIM(biresus3), '') AS biresus3,
		NULLIF(TRIM(delstat3), '') AS delstat3,
		CASE 
			WHEN TRIM(birorder3) = '' THEN NULL
			WHEN birorder3 IS NULL THEN NULL
			WHEN birorder3 = 'X' THEN '9'
			ELSE birorder3
		END AS birorder3,
		NULLIF(TRIM(birstat3), '') AS birstat3,
		NULLIF(TRIM(birweit3), '') AS birweit3,
		NULLIF(TRIM(delmeth3), '') AS delmeth3,
		NULLIF(TRIM(delplace3), '') AS delplace3,
		NULLIF(TRIM(gestat3), '') AS gestat3,
		NULLIF(TRIM(sexbaby3), '') AS sexbaby3
	FROM
		tmp_maternity_episode_data;

	ALTER TABLE original_maternity_episode_data ADD PRIMARY KEY(year, file_row);

END;
$$   LANGUAGE plpgsql;



/*
 * ===============================================================================
 * FUNCTION: load_sample_maternity_data
 * ------------------------------------
 * This function combines various activities related to data cleaning, including
 * basic activities such as:
 * (1) standardising the representation of null values (eg: replacing empty 
 *     strings with null values
 * (2) standardising the representation of some fields (eg: replacing 'M' with 1
 *     and 'F' with 2 to make it comply with HES Inpatient schema
 * (3) casting text field values to more useful types (eg: casting epiorder from
 *     a text field to an integer field
 * (4) de-duplication, based on multiple approaches: preserving the first 
 *     duplicate and preserving the duplicate with the most non-null fields
 * 
 * None of these activities are the main focus of this code example, but they
 * help set-up the part of the code that assesses a data quality score for each
 * record.
 *
 * The kinds of data cleaning you would do on HES records depends on what state 
 * you find them in and what kinds of checks you may want to do.  Typically I 
 * import most fields as type text to make it easier to do search and replace 
 * activities on fields.
 * 
 * Standardising the representation of empty fields 
 * (use NULL for NULL,'', ' ' etc.)
 * field values.
 *
 * Casting text fields to other data types is done to help make the data quality 
 * scoring code easier.  Many of the fields have numerical codes and it is easier 
 * to specify ranges if they're numbers than to use if...then...else or CASE 
 * statements to consider each value represented as a string.  For example, 
 * "x BETWEEN 1 AND 4" 
 * is easier to read than 
 * "IF x = '1' OR x = '2' OR x = '3' OR x = '4'".
 *
 * Casting can present some risks because if you don't catch all cleaning 
 * problems with text-based search and replace, you might end up failing
 * a casting operation and stop the program.  For example, if you don't consider
 * that a code can be 'X' as well as a single digit number, then casting a field
 * value with 'X' to an integer type will fail.
 *
 * De-duplication is an important data cleaning activity, but it will later be
 * viewed as part of a data quality score as well.  Here, we identify duplicates
 * based on a set of fields that together can determine whether two records should
 * be considered the same.  Then we develop two 'keep' flags, either of which
 * could be used to preserve one duplicate and ignore others.  In our approach
 * here, duplicate records are not deleted but are marked as being deleted
 * ===============================================================================
 */
CREATE OR REPLACE FUNCTION clean_maternity_data()
	RETURNS VOID AS 
$$
DECLARE
	
BEGIN

	/*
	 * note that sex and ethnos are good examples of where 
	 * data cleaning could probably be applied productively.
	 * With sex, it's obvious 'M' means male and 'F' means 
	 * female, so if any field values somehow have these values,
	 * then it's probably a safe bet they can be mapped to 1 and 2 
	 * respectively. Ethnos shows a change in ethnic classifications
	 * and a domain expert would have to provide input about how
	 * data sets spanning multiple years and having different coding
	 * systems can be harmonised to some canonical value
	 */
	DROP TABLE IF EXISTS cln_maternity_episode_data;
	CREATE TABLE cln_maternity_episode_data AS
	SELECT
		file_row,
		year,
		description,
		extract_hes_id,
		procode,
		epistart,
		epiend,
		epiorder,
		dob,
		ethnos,
		admidate,
		CASE
			WHEN TRIM(sex) = '' THEN NULL
			WHEN sex = 'M' THEN '1'
			WHEN sex = 'F' THEN '2'
			WHEN sex = 'U' THEN '0'
			ELSE sex
		END AS sex,
		epitype,
		matage,
		numbaby,
		dobbaby1,
		biresus1,
		delstat1,
		CASE 
			WHEN TRIM(birorder1) = '' THEN NULL
			WHEN birorder1 IS NULL THEN NULL
			WHEN birorder1 = 'X' THEN '9'
			ELSE birorder1
		END AS birorder1,
		birstat1,
		birweit1,
		delmeth1,
		delplace1,
		gestat1,
		sexbaby1,
		dobbaby2,
		biresus2,
		delstat2,
		CASE 
			WHEN TRIM(birorder2) = '' THEN NULL
			WHEN birorder2 IS NULL THEN NULL
			WHEN birorder2 = 'X' THEN '9'
			ELSE birorder2
		END AS birorder2,
		birstat2,
		birweit2,
		delmeth2,
		delplace2,
		gestat2,
		sexbaby2,
		dobbaby3,
		biresus3,
		delstat3,
		CASE 
			WHEN TRIM(birorder3) = '' THEN NULL
			WHEN birorder3 IS NULL THEN NULL
			WHEN birorder3 = 'X' THEN '9'
			ELSE birorder3
		END AS birorder3,
		birstat3,
		birweit3,
		delmeth3,
		delplace3,
		gestat3,
		sexbaby3
	FROM
		original_maternity_episode_data
	ORDER BY
		year,
		file_row; 
	ALTER TABLE cln_maternity_episode_data ADD PRIMARY KEY(year, file_row);


	/*
	 * De-duplicate records, picking a set of fields that can be used
	 * to determine whether two episode records are the same
	 */ 
	DROP TABLE IF EXISTS tmp_deduplication1;
	CREATE TABLE tmp_deduplication1 AS
	SELECT
		file_row,
		year,
		description,
		extract_hes_id,
		procode,
		epistart,
		epiend,
		epiorder::INT,
		rank() OVER (ORDER BY 
			extract_hes_id, 
			procode, 
			epistart, 
			epiend,
			epiorder) AS record_family_id,
		dob,
		ethnos,	
		admidate,
		sex::INT,
		epitype::INT,
		matage::INT,	
		numbaby::INT,
		dobbaby1,
		biresus1::INT,
		delstat1::INT,
		birorder1::INT,
		birstat1::INT,
		birweit1::INT,
		delmeth1,
		delplace1::INT,
		gestat1::INT,
		sexbaby1::INT,
		dobbaby2,
		biresus2::INT,
		delstat2::INT,
		birorder2::INT,
		birstat2::INT,
		birweit2::INT,
		delmeth2,
		delplace2::INT,
		gestat2::INT,
		sexbaby2::INT,
		dobbaby3,
		biresus3::INT,
		delstat3::INT,
		birorder3::INT,
		birstat3::INT,
		birweit3::INT,
		delmeth3,
		delplace3::INT,
		gestat3::INT,
		sexbaby3::INT
	FROM
		cln_maternity_episode_data
	ORDER BY
		year,
		file_row; 
	ALTER TABLE tmp_deduplication1 ADD PRIMARY KEY(year, file_row);

	/*
	 * Here we're doing the initial work for trying to identify and flag 
	 * duplicates that should be kept. There are varying ways to code this.
	 * Here cleaning, casting and de-duplicating are done through a
	 * succession of temporary tables.  The nature of these queries is 
	 * easier to explain in this way but they may not perform as well as 
	 * they would if they were combined into fewer but better performing
	 * queries.  But the main purpose of this code demonstration is to 
	 * highlight the nature of scoring, and not to optimise data cleaning
	 * activities. 
	 */
	DROP TABLE IF EXISTS tmp_deduplication2;
	CREATE TABLE tmp_deduplication2 AS
	WITH record_family_id_counts AS
		(SELECT
			record_family_id,
			COUNT(record_family_id) AS total_duplicates
		 FROM
		 	tmp_deduplication1
		 GROUP BY
		 	record_family_id),
	scores AS 
		(SELECT
			file_row,
			year,
			record_family_id,
			CASE
				WHEN description IS NULL THEN 0
				ELSE 1
			END as score_description,
			CASE
				WHEN extract_hes_id IS NULL THEN 0
				ELSE 1
			END as score_extract_hes_id,
			CASE
				WHEN procode IS NULL THEN 0
				ELSE 1
			END as score_procode,
			CASE
				WHEN epistart IS NULL THEN 0
				ELSE 1
			END as score_epistart,
			CASE
				WHEN epiend IS NULL THEN 0
				ELSE 1
			END as score_epiend,
			CASE
				WHEN epiorder IS NULL THEN 0
				ELSE 1
			END as score_epiorder,
			CASE
				WHEN record_family_id IS NULL THEN 0
				ELSE 1
			END as score_record_family_id,
			row_number() OVER (
				PARTITION BY 
					record_family_id
				ORDER BY 
					record_family_id) AS ith_duplicate,			
			CASE
				WHEN record_family_id IS NULL THEN 0
				ELSE 1
			END as score_record_family_id,
			CASE
				WHEN dob IS NULL THEN 0
				ELSE 1
			END as score_dob,
			CASE
				WHEN ethnos IS NULL THEN 0
				ELSE 1
			END as score_ethnos,
			CASE
				WHEN admidate IS NULL THEN 0
				ELSE 1
			END as score_admidate,
			CASE
				WHEN sex IS NULL THEN 0
				ELSE 1
			END as score_sex,
			CASE
				WHEN epitype IS NULL THEN 0
				ELSE 1
			END as score_epitype,
			CASE
				WHEN matage IS NULL THEN 0
				ELSE 1
			END as score_matage,
			CASE
				WHEN numbaby IS NULL THEN 0
				ELSE 1
			END as score_numbaby,
			CASE
				WHEN dobbaby1 IS NULL THEN 0
				ELSE 1
			END as score_dobbaby1,
			CASE
				WHEN biresus1 IS NULL THEN 0
				ELSE 1
			END as score_biresus1,
			CASE
				WHEN delstat1 IS NULL THEN 0
				ELSE 1
			END as score_delstat1,
			CASE
				WHEN birorder1 IS NULL THEN 0
				ELSE 1
			END as score_birorder1,
			CASE
				WHEN birstat1 IS NULL THEN 0
				ELSE 1
			END as score_birstat1,
			CASE
				WHEN birweit1 IS NULL THEN 0
				ELSE 1
			END as score_birweit1,
			CASE
				WHEN delmeth1 IS NULL THEN 0
				ELSE 1
			END as score_delmeth1,
			CASE
				WHEN delplace1 IS NULL THEN 0
				ELSE 1
			END as score_delplace1,
			CASE
				WHEN gestat1 IS NULL THEN 0
				ELSE 1
			END as score_gestat1,
			CASE
				WHEN sexbaby1 IS NULL THEN 0
				ELSE 1
			END as score_sexbaby1,
			CASE
				WHEN dobbaby2 IS NULL THEN 0
				ELSE 1
			END as score_dobbaby2,
			CASE
				WHEN biresus2 IS NULL THEN 0
				ELSE 1
			END as score_biresus2,
			CASE
				WHEN delstat2 IS NULL THEN 0
				ELSE 1
			END as score_delstat2,
			CASE
				WHEN birorder2 IS NULL THEN 0
				ELSE 1
			END as score_birorder2,
			CASE
				WHEN birstat2 IS NULL THEN 0
				ELSE 1
			END as score_birstat2,
			CASE
				WHEN birweit2 IS NULL THEN 0
				ELSE 1
			END as score_birweit2,
			CASE
				WHEN delmeth2 IS NULL THEN 0
				ELSE 1
			END as score_delmeth2,
			CASE
				WHEN delplace2 IS NULL THEN 0
				ELSE 1
			END as score_delplace2,
			CASE
				WHEN gestat2 IS NULL THEN 0
				ELSE 1
			END as score_gestat2,
			CASE
				WHEN sexbaby2 IS NULL THEN 0
				ELSE 1
			END as score_sexbaby2,
			CASE
				WHEN dobbaby3 IS NULL THEN 0
				ELSE 1
			END as score_dobbaby3,
			CASE
				WHEN biresus3 IS NULL THEN 0
				ELSE 1
			END as score_biresus3,
			CASE
				WHEN delstat3 IS NULL THEN 0
				ELSE 1
			END as score_delstat3,
			CASE
				WHEN birorder3 IS NULL THEN 0
				ELSE 1
			END as score_birorder3,
			CASE
				WHEN birstat3 IS NULL THEN 0
				ELSE 1
			END as score_birstat3,
			CASE
				WHEN birweit3 IS NULL THEN 0
				ELSE 1
			END as score_birweit3,
			CASE
				WHEN delmeth3 IS NULL THEN 0
				ELSE 1
			END as score_delmeth3,
			CASE
				WHEN delplace3 IS NULL THEN 0
				ELSE 1
			END as score_delplace3,
			CASE
				WHEN gestat3 IS NULL THEN 0
				ELSE 1
			END as score_gestat3,
			CASE
				WHEN sexbaby3 IS NULL THEN 0
				ELSE 1
			END as score_sexbaby3
		FROM
			tmp_deduplication1)
	SELECT
			a.file_row,
			a.year,
			a.record_family_id,
			a.ith_duplicate,
			CASE
				WHEN b.total_duplicates > 0 THEN
					'Y'
				ELSE
					'N'
			END AS is_duplicate,
			a.score_description +
			a.score_extract_hes_id + 
			a.score_procode + 
			a.score_epistart + 
			a.score_epiend + 
			a.score_epiorder + 
			a.score_dob + 
			a.score_ethnos + 
			a.score_admidate + 
			a.score_sex + 
			a.score_epitype + 
			a.score_matage + 
			a.score_numbaby + 
			a.score_dobbaby1 + 
			a.score_biresus1 + 
			a.score_delstat1 + 
			a.score_birorder1 + 
			a.score_birstat1 + 
			a.score_birweit1 + 
			a.score_delmeth1 + 
			a.score_delplace1 + 
			a.score_gestat1 + 
			a.score_sexbaby1 + 
			a.score_dobbaby2 + 
			a.score_biresus2 + 
			a.score_delstat2 + 
			a.score_birorder2 + 
			a.score_birstat2 + 
			a.score_birweit2 + 
			a.score_delmeth2 + 
			a.score_delplace2 + 
			a.score_gestat2 + 
			a.score_sexbaby2 + 
			a.score_dobbaby3 + 
			a.score_biresus3 + 
			a.score_delstat3 + 
			a.score_birorder3 + 
			a.score_birstat3 + 
			a.score_birweit3 + 
			a.score_delmeth3 + 
			a.score_delplace3 + 
			a.score_gestat3 + 
			a.score_sexbaby3 AS num_filled_fields
	FROM 
		scores a,
		record_family_id_counts b
	WHERE
		a.record_family_id = b.record_family_id;
	ALTER TABLE tmp_deduplication2 ADD PRIMARY KEY(year, file_row);

	DROP TABLE IF EXISTS tmp_deduplication3;
	CREATE TABLE tmp_deduplication3 AS
	WITH maximum_filled_fields AS
		(SELECT
			record_family_id,
			MAX(num_filled_fields) AS max_filled_fields
		 FROM
		 	tmp_deduplication2
		 GROUP BY
		 	record_family_id)
	SELECT
		b.file_row,
		b.year,
		b.num_filled_fields,
		b.is_duplicate,
		CASE 
			WHEN b.ith_duplicate = 1 THEN 'Y'
			ELSE 'N'
		END AS keep_first_duplicate,
		CASE
			WHEN b.num_filled_fields = a.max_filled_fields THEN 'Y'
			ELSE 'N'
		END AS keep_max_filled_duplicate
	FROM
		maximum_filled_fields a,
		tmp_deduplication2 b
	WHERE
		a.record_family_id = b.record_family_id
	ORDER BY
		b.year,
		b.file_row; 
	ALTER TABLE tmp_deduplication3 ADD PRIMARY KEY(year, file_row);
	
	DROP TABLE IF EXISTS fin_deduplication;
	CREATE TABLE fin_deduplication AS
	SELECT
			a.file_row,
			a.year,
			b.num_filled_fields,
			b.is_duplicate,			
			a.description,
			a.extract_hes_id,
			a.procode,
			a.epistart,
			a.epiend,
			a.epiorder,
			a.record_family_id,
			a.dob,
			a.ethnos,	
			a.admidate,
			a.sex,
			a.epitype,
			a.matage,	
			a.numbaby,
			a.dobbaby1,
			a.biresus1,
			a.delstat1,
			a.birorder1,
			a.birstat1,
			a.birweit1,
			a.delmeth1,
			a.delplace1,
			a.gestat1,
			a.sexbaby1,
			a.dobbaby2,
			a.biresus2,
			a.delstat2,
			a.birorder2,
			a.birstat2,
			a.birweit2,
			a.delmeth2,
			a.delplace2,
			a.gestat2,
			a.sexbaby2,
			a.dobbaby3,
			a.biresus3,
			a.delstat3,
			a.birorder3,
			a.birstat3,
			a.birweit3,
			a.delmeth3,
			a.delplace3,
			a.gestat3,
			a.sexbaby3,
			b.keep_first_duplicate,
			b.keep_max_filled_duplicate
	FROM
		tmp_deduplication1 a,
		tmp_deduplication3 b
	WHERE
		a.year = b.year AND
		a.file_row = b.file_row
	ORDER BY
		a.year,
		b.file_row; 
	ALTER TABLE fin_deduplication ADD PRIMARY KEY(year, file_row);

	DROP TABLE IF EXISTS tmp_deduplication1;
	DROP TABLE IF EXISTS tmp_deduplication2;
	DROP TABLE IF EXISTS tmp_deduplication3;


END;
$$   LANGUAGE plpgsql;


/**
 * This function is an over-simplified version of code that would 
 * assess data quality for a set of four baby-related field values
 * that could be related to birth weight.  For live births (birstat = '1')
 * a combination of gestational age at birth (gestat), the sex (sexbaby)
 * will result in a range of birth weights.  The idea for this function
 * is that taken together, we can identify records where the baby's
 * weight is realistic/unrealistic given sex, gestation age at birth.
 * 
 * It provides an example of where individually, a gestat value can be
 * valid and a birweit can be valid but the combination results in something
 * that shouldn't be considered valid.
 */


CREATE OR REPLACE FUNCTION intra_score_realistic_birth_weight(
	birstat INT,
	gestat INT,
	sexbaby INT,
	birweit INT)
	RETURNS INT AS 
$$
DECLARE

BEGIN

	/*
	 * We repeat here the general scoring system used for an intra-field
	 * check.  I've included all eight values on our scale, but some may
	 * not be applicable.  In other cases, you may decide that it is applicable.
	 * The scoring shows that although you can reference a standard scoring 
	 * scale, it is ultimately a judgement call if some are included and
	 * others are ignored.  This is a future area of discussion.
	 * 	 
	 * Scoring System
	 * 1 = illegal value
	 * 2 = medically infeasible
	 * 3 = missing or invalid
	 * 4 = "Unknown values" (Not used in this check)
	 * 5 = "Not specified or "Not applicable" (Not used in this check)
	 * 6 = "Other values" (Not used in this check)
	 * 7 = Medically valid but doubtful values (Not used in this check)
	 * 8 = any other valid value
	 */

	/*
	 * Note that this function is not optimised for performance but for
	 * tutorial purposes of explaining the basic concepts in a larger work.
	 */

	/*
	 * Simplify code by first dealing with obvious invalid cases
	 */
	-- If any of the parameters are null, we can't really make a valid 
	-- assessment
	IF (birstat IS NULL OR
	    gestat IS NULL OR
	    sexbaby IS NULL OR
	    birweit IS NULL) THEN

		RETURN 3;	
	END IF;

	/*
	 * Here we try to cast each of the parameter values to integers
	 * In early processing, we assume all of the fields are text so that
	 * we can more easily identify field values that may have clearly invalid
	 * values like a letter where there should be a number.  For the sake of
	 * explaining the approach, we will assume here that all of them can be
	 * cast to an integer.  We could modify the function to check if the values
	 * are valid integers and return a 3 if any of them were not.
	 *
	 * For the sake of demonstration, we'll assume they can!
	 */	
	
	-- Schema values indicate an invalid value in at least one parameter
	IF  birstat < 1 OR
		birstat > 4 OR
		sexbaby < 1 OR
		sexbaby > 2 OR
		gestat > 49 THEN
		
		RETURN 3;	
	END IF;

	-- Estimates for reasonable birth weight given a sex and gestational age
	-- at birth are only meaningful for live births.  Therefore, if it isn't
	-- a live birth, then we will assume the weight is valid.
	IF birstat != 1 THEN
		RETURN 8;
	END IF;

	/* 
	 * It is possible to have a baby that is 7000g or more but this seems
	 * unlikely.  HES allows it, but we'll assume it is a medically 
	 * infeasible value and return a score of 7.  This is an example of a 
	 * judgement call that would be refined by a domain scientist.
	 */
	IF (birweit > 7000) THEN
		RETURN 7;
	END IF;
	
	/*
	 * Below is an incomplete set of assessments that are meant to be 
	 * borrowing established medical trends on reasonable birth weight
	 * ranges. The sample values here are based on a very comprehensive
	 * table of values appearing in "Centile charts for birthweight for 
	 * gestational age for Scottish singleton births" by Sandra Bonellie, 
	 * James Chalmers, Ron Gray, Ian Greer, Stephen Jarvis and Claire 
	 * Williams.  Note that these tables may or may not differ for other
	 * countries or areas of the world.  It just provides an example here.	 
	 */ 
	IF sexbaby = 1 AND 
		gestat = 24 AND
	   	(birweit < 326 OR birweit > 944) THEN	   	   
		RETURN 2;
	ELSIF sexbaby = 1 AND
		gestat = 25 AND
		(birweit < 379 OR birweit > 1080) THEN
		RETURN 2;
	ELSIF sexbaby = 1 AND
		gestat = 26 AND
		(birweit < 430 OR birweit > 1207) THEN
		RETURN 2;
	ELSIF sexbaby = 1 AND
		gestat = 42 AND
		(birweit < 2935 OR birweit > 4748) THEN
		RETURN 2;
	ELSIF sexbaby = 1 AND
		gestat = 43 AND
		(birweit < 2976 OR birweit > 4781) THEN
		RETURN 2;		
	ELSIF sexbaby = 2 AND 
		gestat = 24 AND
	   	(birweit < 270 OR birweit > 916) THEN	   	   
		RETURN 2;
	ELSIF sexbaby = 2 AND
		gestat = 25 AND
		(birweit < 320 OR birweit > 1044) THEN
		RETURN 2;
	ELSIF sexbaby = 2 AND
		gestat = 26 AND
		(birweit < 382 OR birweit > 1208) THEN
		RETURN 2;
	ELSIF sexbaby = 2 AND
		gestat = 42 AND
		(birweit < 2935 OR birweit > 4748) THEN
		RETURN 2;
	ELSIF sexbaby = 2 AND
		gestat = 43 AND
		(birweit < 2909 OR birweit > 4560) THEN
		RETURN 2;
	ELSE 
		RETURN 8;
	END IF;

END;
$$   LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION assess_field_and_intra_record_quality()
	RETURNS VOID AS 
$$
DECLARE
	
BEGIN

	DROP TABLE IF EXISTS fin_field_intra_record_checks;
	CREATE TABLE fin_field_intra_record_checks AS
	SELECT
		year,
		file_row,
		CASE
			WHEN year IS NULL THEN 3 -- blank
			ELSE 8 -- non-blank, as we would expect
		END AS dq_field_year,
		CASE	
			WHEN dob IS NULL THEN 3 -- blank
			WHEN (year - EXTRACT(YEAR FROM dob) >= 60) THEN 2 --medically infeasible
			ELSE 8
		END AS dq_field_dob,	
		CASE
			WHEN ethnos IS NULL THEN 3 -- blank
			WHEN ethnos = 'X' THEN 4 --not known
			WHEN ethnos = ANY('{9, Z}') THEN 5 --not stated
			WHEN ethnos = ANY('{8, S}') THEN 6 -- any other ethnic group
			WHEN ethnos = ANY('{1,2,3,4,5,6,7,A,B,C,D,E,F,G,H,J,K,L,M,N,P,R}') THEN 8
			ELSE 1
		END AS dq_field_ethnos,
		CASE
			WHEN admidate IS NULL THEN 3
			WHEN (EXTRACT(YEAR FROM admidate) != year) THEN 2 -- unrealistic value
			ELSE 8
		END AS dq_field_admidate,
		CASE
			WHEN procode IS NULL THEN 3  -- blank
			ELSE 8
		END AS dq_field_procode,
		CASE
			WHEN sex IS NULL THEN 3 -- blank
			WHEN sex = 1 THEN 2 -- medically impossible, male
			WHEN sex = 0 THEN 4 -- not known
			WHEN sex = 9 THEN 5 -- not specified
			WHEN sex = 2 THEN 8 -- female is the only value that should appear
			ELSE 1 --illegal
		END AS dq_field_sex,
		CASE
			WHEN epitype IS NULL THEN 3 -- blank
			WHEN epitype BETWEEN 1 AND 6 THEN 8 -- [1,6]
			ELSE 1
		END AS dq_field_epitype,
		CASE
			WHEN epistart IS NULL THEN 3 -- blank
			ELSE 8
		END AS dq_field_epistart,
		CASE
			WHEN epiend IS NULL THEN 3 -- blank
			ELSE 8
		END AS dq_field_epiend,
		CASE -- do we expect epiorder to be anything other than 1 for pregnancies??
			WHEN epiorder IS NULL THEN 3 -- not applicable, other maternity event
			WHEN epiorder = 99 THEN 4 -- not known
			WHEN epiorder = 98 THEN 5 -- not applicable
			WHEN epiorder BETWEEN 1 AND 87 THEN 8
			ELSE 1 --illegal
		END AS dq_field_epiorder,
		CASE
			WHEN matage IS NULL THEN 3
			WHEN matage = 0 THEN 1 --illogical age for a mother
			WHEN matage = 110 THEN 1
			ELSE 8
		END AS dq_field_matage,
		CASE
			WHEN numbaby IS NULL THEN 3 -- blank
			WHEN numbaby = 9 THEN 4 -- not known
			WHEN numbaby = 6 THEN 2 --medically infeasible because of distribution
			WHEN numbaby BETWEEN 1 AND 5 THEN 8 -- [1,5]
			ELSE 1 --illegal
		END AS dq_field_numbaby, -- dq_field_numbaby

		CASE
			WHEN numbaby < 1 AND dobbaby1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND dobbaby1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND dobbaby1 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND dobbaby1 < admidate THEN 1 -- illegal
			ELSE 8
		END AS dq_intra_dobbaby1,
		CASE
			WHEN numbaby < 1 AND delstat1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delstat1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delstat1 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND delstat1 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND delstat1 = 8 THEN 6 -- other
			WHEN numbaby >= 1 AND delstat1 BETWEEN 1 AND 3 THEN 8 
			ELSE 1
		END AS dq_intra_delstat1,
		CASE
			WHEN numbaby < 1 AND biresus1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND biresus1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND biresus1 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND biresus1 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND biresus1 = 8 THEN 5 -- not applicable
			WHEN numbaby >= 1 AND biresus1 BETWEEN 1 AND 6 THEN 8 
			ELSE 1
		END AS dq_intra_biresus1,
		CASE
			WHEN numbaby < 1 AND birorder1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birorder1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birorder1 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birorder1 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND birorder1 = 8 THEN 5 -- not applicable
			WHEN numbaby >= 1 AND birorder1 = ANY('{1,2,3,4,5,6,7}') THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_birorder1,
		CASE
			WHEN numbaby < 1 AND birstat1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birstat1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birstat1 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birstat1 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND birstat1 = ANY('{1,2,3,4}') THEN 8
			ELSE 1 --illegal value
		END AS dq_intra_birstat1,
		CASE
			WHEN numbaby < 1 AND birweit1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birweit1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birweit1 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birweit1 < 0 THEN 1
			WHEN numbaby >= 1 AND birweit1 BETWEEN 0 AND 200 THEN 2 -- unrealistic
			WHEN numbaby >= 1 AND birweit1 BETWEEN 5000 AND 7000 THEN 2 -- unrealistic
			WHEN numbaby >= 1 AND birweit1 = 9999 THEN 4 --not known
			WHEN numbaby >= 1 AND birweit1 BETWEEN 7001 AND 9998 THEN 1 -- no values between 7000 and 9999 allowed
			ELSE 8
		END AS dq_intra_birweit1,
		CASE
			WHEN numbaby < 1 AND delmeth1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delmeth1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delmeth1 IS NULL THEN 3 
			WHEN numbaby >= 1 AND delmeth1 = 'X' THEN 4 -- Not Known
			WHEN numbaby >= 1 AND delmeth1 = '9' THEN 6 -- Other
			WHEN numbaby >= 1 AND delmeth1 = ANY('{0,1,2,3,4,5,6,7,8}') THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_delmeth1,
		CASE
			WHEN numbaby < 1 AND delplace1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delplace1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delplace1 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND delplace1 = 9 THEN 4 -- Not Known
			WHEN numbaby >= 1 AND delplace1 = 8 THEN 6 -- Other category
			WHEN numbaby >= 1 AND delplace1 BETWEEN 0 AND 7 THEN 8 
			ELSE 1 --illegal
		END AS dq_intra_delplace1,
		CASE
			WHEN numbaby < 1 AND gestat1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND gestat1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND gestat1 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND gestat1 = 99 THEN 4 -- not known
			WHEN numbaby >= 1 AND gestat1 BETWEEN 10 AND 49 THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_gestat1,
		CASE
			WHEN numbaby < 1 AND sexbaby1 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND sexbaby1 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND sexbaby1 IS NULL THEN 3 -- blank	
			WHEN numbaby >= 1 AND sexbaby1 = 0 THEN 4 -- Not known
			WHEN numbaby >= 1 AND sexbaby1= 9  THEN 5 -- Not specified
			WHEN numbaby >= 1 AND sexbaby1 = ANY('{1,2}') THEN 8 -- legal values
			ELSE 1 -- illegal
		END AS dq_intra_sexbaby1,
		CASE
			WHEN numbaby < 1 AND dobbaby2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND dobbaby2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND dobbaby2 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND dobbaby2 < admidate THEN 1 -- illegal
			ELSE 8
		END AS dq_intra_dobbaby2,
		CASE
			WHEN numbaby < 1 AND delstat2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delstat2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delstat2 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND delstat2 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND delstat2 = 8 THEN 6 -- other
			WHEN numbaby >= 1 AND delstat2 BETWEEN 1 AND 3 THEN 8 
			ELSE 1
		END AS dq_intra_delstat2,
		CASE
			WHEN numbaby < 1 AND biresus2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND biresus2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND biresus2 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND biresus2 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND biresus2 = 8 THEN 5 -- not applicable
			WHEN numbaby >= 1 AND biresus2 BETWEEN 1 AND 6 THEN 8 
			ELSE 1
		END AS dq_intra_biresus2,
		CASE
			WHEN numbaby < 1 AND birorder2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birorder2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birorder2 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birorder2 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND birorder2 = 8 THEN 5 -- not applicable
			WHEN numbaby >= 1 AND birorder2 = ANY('{1,2,3,4,5,6,7}') THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_birorder2,
		CASE
			WHEN numbaby < 1 AND birstat2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birstat2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birstat2 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birstat2 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND birstat2 = ANY('{1, 2, 3, 4}') THEN 8
			ELSE 1 --illegal value
		END AS dq_intra_birstat2,
		CASE
			WHEN numbaby < 1 AND birweit2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birweit2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birweit2 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birweit2 < 0 THEN 1
			WHEN numbaby >= 1 AND birweit2 BETWEEN 0 AND 200 THEN 2 -- unrealistic
			WHEN numbaby >= 1 AND birweit2 BETWEEN 5000 AND 7000 THEN 2 -- unrealistic
			WHEN numbaby >= 1 AND birweit2 = 9999 THEN 4 --not known
			WHEN numbaby >= 1 AND birweit2 BETWEEN 7001 AND 9998 THEN 1 -- no values between 7000 and 9999 allowed
			ELSE 8
		END AS dq_intra_birweit2,
		CASE
			WHEN numbaby < 1 AND delmeth2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delmeth2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delmeth2 IS NULL THEN 3 
			WHEN numbaby >= 1 AND delmeth2 = 'X' THEN 4 -- Not Known
			WHEN numbaby >= 1 AND delmeth2 = '9' THEN 6 -- Other
			WHEN numbaby >= 1 AND delmeth2 = ANY('{0,1,2,3,4,5,6,7,8}') THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_delmeth2,
		CASE
			WHEN numbaby < 1 AND delplace2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delplace2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delplace2 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND delplace2 = 9 THEN 4 -- Not Known
			WHEN numbaby >= 1 AND delplace2 = 8 THEN 6 -- Other category
			WHEN numbaby >= 1 AND delplace2 BETWEEN 0 AND 7 THEN 8 
			ELSE 1 --illegal
		END AS dq_intra_delplace2,
		CASE
			WHEN numbaby < 1 AND gestat2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND gestat2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND gestat2 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND gestat2 = 99 THEN 4 -- not known
			WHEN numbaby >= 1 AND gestat2 BETWEEN 10 AND 49 THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_gestat2,
		CASE
			WHEN numbaby < 1 AND sexbaby2 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND sexbaby2 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND sexbaby2 IS NULL THEN 3 -- blank	
			WHEN numbaby >= 1 AND sexbaby2 = 0 THEN 4 -- Not known
			WHEN numbaby >= 1 AND sexbaby2 = 9  THEN 5 -- Not specified
			WHEN numbaby >= 1 AND sexbaby2 = ANY('{1,2}') THEN 8 -- legal values
			ELSE 1 -- illegal
		END AS dq_intra_sexbaby2,

		CASE
			WHEN numbaby < 1 AND dobbaby3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND dobbaby3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND dobbaby3 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND dobbaby3 < admidate THEN 1 -- illegal
			ELSE 8
		END AS dq_intra_dobbaby3,
		CASE
			WHEN numbaby < 1 AND delstat3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delstat3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delstat3 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND delstat3 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND delstat3 = 8 THEN 6 -- other
			WHEN numbaby >= 1 AND delstat3 BETWEEN 1 AND 3 THEN 8 
			ELSE 1
		END AS dq_intra_delstat3,
		CASE
			WHEN numbaby < 1 AND biresus3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND biresus3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND biresus3 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND biresus3 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND biresus3 = 8 THEN 5 -- not applicable
			WHEN numbaby >= 1 AND biresus3 BETWEEN 1 AND 6 THEN 8 
			ELSE 1
		END AS dq_intra_biresus3,
		CASE
			WHEN numbaby < 1 AND birorder3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birorder3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birorder3 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birorder3 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND birorder3 = 8 THEN 5 -- not applicable
			WHEN numbaby >= 1 AND birorder3 = ANY('{1,2,3,4,5,6,7}') THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_birorder3,
		CASE
			WHEN numbaby < 1 AND birstat3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birstat3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birstat3 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birstat3 = 9 THEN 4 -- not known
			WHEN numbaby >= 1 AND birstat3 = ANY('{1, 2, 3, 4}') THEN 8
			ELSE 1 --illegal value
		END AS dq_intra_birstat3,
		CASE
			WHEN numbaby < 1 AND birweit3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND birweit3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND birweit3 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND birweit3 < 0 THEN 1
			WHEN numbaby >= 1 AND birweit3 BETWEEN 0 AND 200 THEN 2 -- unrealistic
			WHEN numbaby >= 1 AND birweit3 BETWEEN 5000 AND 7000 THEN 2 -- unrealistic
			WHEN numbaby >= 1 AND birweit3 = 9999 THEN 4 --not known
			WHEN numbaby >= 1 AND birweit3 BETWEEN 7001 AND 9998 THEN 1 -- no values between 7000 and 9999 allowed
			ELSE 8
		END AS dq_intra_birweit3,
		CASE
			WHEN numbaby < 1 AND delmeth3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delmeth3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delmeth3 IS NULL THEN 3 
			WHEN numbaby >= 1 AND delmeth3 = 'X' THEN 4 -- Not Known
			WHEN numbaby >= 1 AND delmeth3 = '9' THEN 6 -- Other
			WHEN numbaby >= 1 AND delmeth3 = ANY('{0,1,2,3,4,5,6,7,8}') THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_delmeth3,
		CASE
			WHEN numbaby < 1 AND delplace3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND delplace3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND delplace3 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND delplace3 = 9 THEN 4 -- Not Known
			WHEN numbaby >= 1 AND delplace3 = 8 THEN 6 -- Other category
			WHEN numbaby >= 1 AND delplace3 BETWEEN 0 AND 7 THEN 8 
			ELSE 1 --illegal
		END AS dq_intra_delplace3,
		CASE
			WHEN numbaby < 1 AND gestat3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND gestat3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND gestat3 IS NULL THEN 3 -- blank
			WHEN numbaby >= 1 AND gestat3 = 99 THEN 4 -- not known
			WHEN numbaby >= 1 AND gestat3 BETWEEN 10 AND 49 THEN 8
			ELSE 1 -- illegal
		END AS dq_intra_gestat3,
		CASE
			WHEN numbaby < 1 AND sexbaby3 IS NULL THEN 8 -- blank, as it should be
			WHEN numbaby < 1 AND sexbaby3 IS NOT NULL THEN 1 -- non-blank 
			WHEN numbaby >= 1 AND sexbaby3 IS NULL THEN 3 -- blank	
			WHEN numbaby >= 1 AND sexbaby3 = 0 THEN 4 -- Not known
			WHEN numbaby >= 1 AND sexbaby3 = 9  THEN 5 -- Not specified
			WHEN numbaby >= 1 AND sexbaby3 = ANY('{1,2}') THEN 8 -- legal values
			ELSE 1 -- illegal
		END AS dq_intra_sexbaby3,
		intra_score_realistic_birth_weight(
			birstat1, 
			gestat1, 
			sexbaby1, 
			birweit1) AS dq_intra_realistic_baby_weight1,
		intra_score_realistic_birth_weight(
			birstat2, 
			gestat2, 
			sexbaby2, 
			birweit2) AS dq_intra_realistic_baby_weight2,
		intra_score_realistic_birth_weight(
			birstat3, 
			gestat3, 
			sexbaby3, 
			birweit3) AS dq_intra_realistic_baby_weight3
	FROM	
		fin_deduplication
	ORDER BY
		year,
		file_row;

END;
$$   LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION assess_inter_record_quality()
	RETURNS VOID AS 
$$
DECLARE
	
BEGIN

	-- first check: determine whether 
	DROP TABLE IF EXISTS tmp_inter_record_checks1;
	CREATE TABLE tmp_inter_record_checks1 AS
	SELECT
		year,
		file_row,
		is_duplicate,
		admidate,
		LAG(admidate) OVER
		(PARTITION BY 
			extract_hes_id
		ORDER BY
			extract_hes_id, 
			admidate) AS previous_pregnancy_admission,
		numbaby,
		CASE
			WHEN numbaby = 1 AND birstat1 = 1 THEN 'Y'
			WHEN numbaby = 2 AND birstat1 = 1 AND birstat2 = 1 THEN 'Y'
			WHEN numbaby = 3 AND birstat1 = 1 AND birstat2 = 1 AND birstat3 = 1 THEN 'Y'
			ELSE 'N'
		END yielded_all_live_births,
		keep_first_duplicate,
		keep_max_filled_duplicate
	FROM
		fin_deduplication;

	DROP TABLE IF EXISTS fin_inter_record_checks;
	CREATE TABLE fin_inter_record_checks AS
	WITH pregnancy_intervals AS
		(SELECT
			year,
			file_row,
			is_duplicate,
			CASE 
				WHEN admidate IS NOT NULL AND previous_pregnancy_admission IS NOT NULL THEN
					(admidate - previous_pregnancy_admission)/7::INT
				ELSE
					NULL
			END AS weeks_between_pregnancies,
			yielded_all_live_births,
			keep_first_duplicate,
			keep_max_filled_duplicate
		 FROM
		 	tmp_inter_record_checks1)
	SELECT
		year,
		file_row,
		CASE
			WHEN yielded_all_live_births = 'N' THEN 8
			WHEN weeks_between_pregnancies IS NULL THEN 8
			WHEN weeks_between_pregnancies BETWEEN 23 AND 25 THEN 7 -- medically unlikely
			WHEN weeks_between_pregnancies < 23 THEN 2 -- medically unlikely
			ELSE 8
		END dq_inter_birth_interval,
		CASE
			WHEN is_duplicate = 'Y' THEN 1
			ELSE 8
		END dq_inter_is_duplicate
	FROM
		pregnancy_intervals;
	DROP TABLE tmp_inter_record_checks1;

	/*
	 * Now create one final table that holds field level, intra-record and inter-record
	 * checks. 
	 */
	 DROP TABLE IF EXISTS fin_dq_maternity_data;
	 CREATE TABLE fin_dq_maternity_data AS
	 SELECT
	 	a.year,
	 	a.file_row,
		a.dq_field_year,
		a.dq_field_dob,	
		a.dq_field_ethnos,
		a.dq_field_admidate,
		a.dq_field_procode,
		a.dq_field_sex,
		a.dq_field_epitype,
		a.dq_field_epistart,
		a.dq_field_epiend,
		a.dq_field_epiorder,
		a.dq_field_matage,
		a.dq_field_numbaby,		
		a.dq_intra_dobbaby1 * 10 AS dq_intra_dobbaby1,
		a.dq_intra_delstat1 * 10 AS dq_intra_delstat1,
		a.dq_intra_biresus1 * 10 AS dq_intra_biresus1,
		a.dq_intra_birorder1 * 10 AS dq_intra_birorder1,
		a.dq_intra_birstat1 * 10 AS dq_intra_birstat1,
		a.dq_intra_birweit1 * 10 AS dq_intra_birweit1,
		a.dq_intra_delmeth1 * 10 AS dq_intra_delmeth1,
		a.dq_intra_delplace1 * 10 AS dq_intra_delplace1,
		a.dq_intra_gestat1 * 10 AS dq_intra_gestat1,
		a.dq_intra_sexbaby1 * 10 AS dq_intra_sexbaby1,
		a.dq_intra_dobbaby2 * 10 AS dq_intra_dobbaby2,
		a.dq_intra_delstat2 * 10 AS dq_intra_delstat2,
		a.dq_intra_biresus2 * 10 AS dq_intra_biresus2,
		a.dq_intra_birorder2 * 10 AS dq_intra_birorder2,
		a.dq_intra_birstat2 * 10 AS dq_intra_birstat2,
		a.dq_intra_birweit2 * 10 AS dq_intra_birweit2,
		a.dq_intra_delmeth2 * 10 AS dq_intra_delmeth2,
		a.dq_intra_delplace2 * 10 AS dq_intra_delplace2,
		a.dq_intra_gestat2 * 10 AS dq_intra_gestat2,
		a.dq_intra_sexbaby2 * 10 AS dq_intra_sexbaby2,
		a.dq_intra_dobbaby3 * 10 AS dq_intra_dobbaby3,
		a.dq_intra_delstat3 * 10 AS dq_intra_delstat3,
		a.dq_intra_biresus3 * 10 AS dq_intra_biresus3,
		a.dq_intra_birorder3 * 10 AS dq_intra_birorder3,
		a.dq_intra_birstat3 * 10 AS dq_intra_birstat3,
		a.dq_intra_birweit3 * 10 AS dq_intra_birweit3,
		a.dq_intra_delmeth3 * 10 AS dq_intra_delmeth3,
		a.dq_intra_delplace3 * 10 AS dq_intra_delplace3,
		a.dq_intra_gestat3 * 10 AS dq_intra_gestat3,
		a.dq_intra_sexbaby3 * 10 AS dq_intra_sexbaby3,
		a.dq_intra_realistic_baby_weight1 * 10 AS dq_intra_realistic_baby_weight1,
		a.dq_intra_realistic_baby_weight2 * 10 AS dq_intra_realistic_baby_weight2,
		a.dq_intra_realistic_baby_weight3 * 10 AS dq_intra_realistic_baby_weight3,	
		b.dq_inter_birth_interval * 100 AS dq_inter_birth_interval,
		b.dq_inter_is_duplicate * 100 AS dq_inter_is_duplicate
	FROM
	 	fin_field_intra_record_checks a,
		fin_inter_record_checks b
	WHERE
	 	a.year = b.year AND 
		a.file_row = b.file_row
	 ORDER BY
	 	a.year,
	 	a.file_row;
		
	ALTER TABLE fin_dq_maternity_data ADD PRIMARY KEY(year, file_row);

	DROP TABLE IF EXISTS fin_field_intra_record_checks;
	DROP TABLE IF EXISTS fin_inter_record_checks;

	 
END;
$$   LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION assess_score_weightings()
	RETURNS VOID AS 
$$
DECLARE
	
BEGIN


	DROP TABLE IF EXISTS fin_dq_maternity_check_weights;
	CREATE TABLE fin_dq_maternity_check_weights AS
	SELECT
	 	year,
		AVG(dq_field_year)/8 AS dq_avg_year,
		AVG(dq_field_dob)/8 AS dq_avg_dob,	
		AVG(dq_field_ethnos)/8 AS dq_avg_ethnos,
		AVG(dq_field_admidate)/8 AS dq_avg_admidate,
		AVG(dq_field_procode)/8 AS dq_avg_procode,
		AVG(dq_field_sex)/8 AS dq_avg_sex,
		AVG(dq_field_epitype)/8 AS dq_avg_epitype,
		AVG(dq_field_epistart)/8 AS dq_avg_epistart,
		AVG(dq_field_epiend)/8 AS dq_avg_epiend,
		AVG(dq_field_epiorder)/8 AS dq_avg_epiorder,
		AVG(dq_field_matage)/8 AS dq_avg_matage,
		AVG(dq_field_numbaby)/8 AS dq_avg_numbaby,
		AVG(dq_intra_dobbaby1)/80 AS dq_avg_dobbaby1,
		AVG(dq_intra_delstat1)/80 AS dq_avg_delstat1,
		AVG(dq_intra_biresus1)/80 AS dq_avg_biresus1,
		AVG(dq_intra_birorder1)/80 AS dq_avg_birorder1,
		AVG(dq_intra_birstat1)/80 AS dq_avg_birstat1,
		AVG(dq_intra_birweit1)/80 AS dq_avg_birweit1,
		AVG(dq_intra_delmeth1)/80 AS dq_avg_delmeth1,
		AVG(dq_intra_delplace1)/80 AS dq_avg_delplace1,
		AVG(dq_intra_gestat1)/80 AS dq_avg_gestat1,
		AVG(dq_intra_sexbaby1)/80 AS dq_avg_sexbaby1,
		AVG(dq_intra_dobbaby2)/80 AS dq_avg_dobbaby2,
		AVG(dq_intra_delstat2)/80 AS dq_avg_delstat2,
		AVG(dq_intra_biresus2)/80 AS dq_avg_biresus2,
		AVG(dq_intra_birorder2)/80 AS dq_avg_birorder2,
		AVG(dq_intra_birstat2)/80 AS dq_avg_birstat2,
		AVG(dq_intra_birweit2)/80 AS dq_avg_birweit2,
		AVG(dq_intra_delmeth2)/80 AS dq_avg_delmeth2,
		AVG(dq_intra_delplace2)/80 AS dq_avg_delplace2,
		AVG(dq_intra_gestat2)/80 AS dq_avg_gestat2,
		AVG(dq_intra_sexbaby2)/80 AS dq_avg_sexbaby2,
		AVG(dq_intra_dobbaby3)/80 AS dq_avg_dobbaby3,
		AVG(dq_intra_delstat3)/80 AS dq_avg_delstat3,
		AVG(dq_intra_biresus3)/80 AS dq_avg_biresus3,
		AVG(dq_intra_birorder3)/80 AS dq_avg_birorder3,
		AVG(dq_intra_birstat3)/80 AS dq_avg_birstat3,
		AVG(dq_intra_birweit3)/80 AS dq_avg_birweit3,
		AVG(dq_intra_delmeth3)/80 AS dq_avg_delmeth3,
		AVG(dq_intra_delplace3)/80 AS dq_avg_delplace3,
		AVG(dq_intra_gestat3)/80 AS dq_avg_gestat3,
		AVG(dq_intra_sexbaby3)/80 AS dq_avg_sexbaby3,
		AVG(dq_intra_realistic_baby_weight1)/80 AS dq_avg_realistic_baby_weight1,
		AVG(dq_intra_realistic_baby_weight2)/80 AS dq_avg_realistic_baby_weight2,
		AVG(dq_intra_realistic_baby_weight3)/80 AS dq_avg_realistic_baby_weight3,		
		AVG(dq_inter_birth_interval)/800 AS dq_avg_birth_interval,
		AVG(dq_inter_is_duplicate)/800 AS dq_avg_is_duplicate
	FROM	
		fin_dq_maternity_data
	GROUP BY
		year
	ORDER BY 
		year;

	ALTER TABLE fin_dq_maternity_check_weights ADD PRIMARY KEY (year); 

	DROP TABLE IF EXISTS fin_weighted_dq_scores;
	CREATE TABLE fin_weighted_dq_scores AS
	SELECT
	 	a.year,
	 	a.file_row,
		a.dq_field_year,
		a.dq_field_dob,	
		a.dq_field_ethnos,
		a.dq_field_admidate,
		a.dq_field_procode,
		a.dq_field_sex,
		a.dq_field_epitype,
		a.dq_field_epistart,
		a.dq_field_epiend,
		a.dq_field_epiorder,
		a.dq_field_matage,
		a.dq_field_numbaby,
		a.dq_intra_dobbaby1,
		a.dq_intra_delstat1,
		a.dq_intra_biresus1,
		a.dq_intra_birorder1,
		a.dq_intra_birstat1,
		a.dq_intra_birweit1,
		a.dq_intra_delmeth1,
		a.dq_intra_delplace1,
		a.dq_intra_gestat1,
		a.dq_intra_sexbaby1,
		a.dq_intra_dobbaby2,
		a.dq_intra_delstat2,
		a.dq_intra_biresus2,
		a.dq_intra_birorder2,
		a.dq_intra_birstat2,
		a.dq_intra_birweit2,
		a.dq_intra_delmeth2,
		a.dq_intra_delplace2,
		a.dq_intra_gestat2,
		a.dq_intra_sexbaby2,
		a.dq_intra_dobbaby3,
		a.dq_intra_delstat3,
		a.dq_intra_biresus3,
		a.dq_intra_birorder3,
		a.dq_intra_birstat3,
		a.dq_intra_birweit3,
		a.dq_intra_delmeth3,
		a.dq_intra_delplace3,
		a.dq_intra_gestat3,
		a.dq_intra_sexbaby3,
		a.dq_intra_realistic_baby_weight1,
		a.dq_intra_realistic_baby_weight2,
		a.dq_intra_realistic_baby_weight3,		
		a.dq_inter_birth_interval,
		a.dq_inter_first_duplicate,
		a.dq_inter_most_filled_duplicate,
		a.dq_field_year + 
		a.dq_field_dob + 	
		a.dq_field_ethnos + 
		a.dq_field_admidate + 
		a.dq_field_procode + 
		a.dq_field_sex + 
		a.dq_field_epitype + 
		a.dq_field_epistart + 
		a.dq_field_epiend + 
		a.dq_field_epiorder + 
		a.dq_field_matage + 
		a.dq_field_numbaby + 
		a.dq_intra_dobbaby1 + 
		a.dq_intra_delstat1 + 
		a.dq_intra_biresus1 + 
		a.dq_intra_birorder1 + 
		a.dq_intra_birstat1 + 
		a.dq_intra_birweit1 + 
		a.dq_intra_delmeth1 + 
		a.dq_intra_delplace1 + 
		a.dq_intra_gestat1 + 
		a.dq_intra_sexbaby1 + 
		a.dq_intra_dobbaby2 + 
		a.dq_intra_delstat2 + 
		a.dq_intra_biresus2 + 
		a.dq_intra_birorder2 + 
		a.dq_intra_birstat2 + 
		a.dq_intra_birweit2 + 
		a.dq_intra_delmeth2 + 
		a.dq_intra_delplace2 + 
		a.dq_intra_gestat2 + 
		a.dq_intra_sexbaby2 + 
		a.dq_intra_dobbaby3 + 
		a.dq_intra_delstat3 + 
		a.dq_intra_biresus3 + 
		a.dq_intra_birorder3 + 
		a.dq_intra_birstat3 + 
		a.dq_intra_birweit3 + 
		a.dq_intra_delmeth3 + 
		a.dq_intra_delplace3 + 
		a.dq_intra_gestat3 + 
		a.dq_intra_sexbaby3 + 
		a.dq_intra_realistic_baby_weight1 + 
		a.dq_intra_realistic_baby_weight2 + 
		a.dq_intra_realistic_baby_weight3 + 		
		a.dq_inter_birth_interval + 
		a.dq_inter_is_duplicate AS unadjusted_total_score,
		a.dq_field_year * b.dq_avg_year + 
		a.dq_field_dob * b.dq_avg_dob + 	
		a.dq_field_ethnos * b.dq_avg_ethnos + 
		a.dq_field_admidate * b.dq_avg_admidate + 
		a.dq_field_procode * b.dq_avg_procode + 
		a.dq_field_sex * b.dq_avg_sex + 
		a.dq_field_epitype * b.dq_avg_epitype + 
		a.dq_field_epistart * b.dq_avg_epistart + 
		a.dq_field_epiend * b.dq_avg_epiend + 
		a.dq_field_epiorder * b.dq_avg_epiorder + 
		a.dq_field_matage * b.dq_avg_matage + 
		a.dq_field_numbaby * b.dq_avg_numbaby + 
		a.dq_intra_dobbaby1 * b.dq_avg_dobbaby1 + 
		a.dq_intra_delstat1 * b.dq_avg_delstat1 + 
		a.dq_intra_biresus1 * b.dq_avg_biresus1 + 
		a.dq_intra_birorder1 * b.dq_avg_birorder1 + 
		a.dq_intra_birstat1 * b.dq_avg_birstat1 + 
		a.dq_intra_birweit1 * b.dq_avg_birweit1 + 
		a.dq_intra_delmeth1 * b.dq_avg_delmeth1 + 
		a.dq_intra_delplace1 * b.dq_avg_delplace1 + 
		a.dq_intra_gestat1 * b.dq_avg_gestat1 + 
		a.dq_intra_sexbaby1 * b.dq_avg_sexbaby1 + 
		a.dq_intra_dobbaby2 * b.dq_avg_dobbaby2 + 
		a.dq_intra_delstat2 * b.dq_avg_delstat2 + 
		a.dq_intra_biresus2 * b.dq_avg_biresus2 + 
		a.dq_intra_birorder2 * b.dq_avg_birorder2 + 
		a.dq_intra_birstat2 * b.dq_avg_birstat2 + 
		a.dq_intra_birweit2 * b.dq_avg_birweit2 + 
		a.dq_intra_delmeth2 * b.dq_avg_delmeth2 + 
		a.dq_intra_delplace2 * b.dq_avg_delplace2 + 
		a.dq_intra_gestat2 * b.dq_avg_gestat2 + 
		a.dq_intra_sexbaby2 * b.dq_avg_sexbaby2 + 
		a.dq_intra_dobbaby3 * b.dq_avg_dobbaby3 + 
		a.dq_intra_delstat3 * b.dq_avg_delstat3 + 
		a.dq_intra_biresus3 * b.dq_avg_biresus3 + 
		a.dq_intra_birorder3 * b.dq_avg_birorder3 + 
		a.dq_intra_birstat3 * b.dq_avg_birstat3 + 
		a.dq_intra_birweit3 * b.dq_avg_birweit3 + 
		a.dq_intra_delmeth3 * b.dq_avg_delmeth3 + 
		a.dq_intra_delplace3 * b.dq_avg_delplace3 + 
		a.dq_intra_gestat3 * b.dq_avg_gestat3 + 
		a.dq_intra_sexbaby3 * b.dq_avg_sexbaby3 + 
		a.dq_intra_realistic_baby_weight1 * b.dq_avg_realistic_baby_weight1 + 
		a.dq_intra_realistic_baby_weight2 * b.dq_avg_realistic_baby_weight2 + 
		a.dq_intra_realistic_baby_weight3 * b.dq_avg_realistic_baby_weight3 + 		
		a.dq_inter_birth_interval * b.dq_avg_birth_interval + 
		a.dq_inter_is_duplicate * b.dq_avg_is_duplicate AS adjusted_total_score		
	FROM
		fin_dq_maternity_data a,
		fin_dq_maternity_check_weights b
	WHERE
		a.year = b.year
	ORDER BY
		a.year,
		a.file_row;






END;
$$   LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION run_scoring_demo()
	RETURNS VOID AS 
$$
DECLARE
	
BEGIN

	PERFORM load_sample_maternity_data();
	PERFORM clean_maternity_data();
	PERFORM assess_field_and_intra_record_quality();
	PERFORM assess_inter_record_quality();
	PERFORM assess_score_weightings();
	
END;
$$   LANGUAGE plpgsql;

SELECT "run_scoring_demo"();
