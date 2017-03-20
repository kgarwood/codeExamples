
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

CREATE OR REPLACE FUNCTION intra_score_birweit(
	birstat INT,
	gestat INT,
	sexbaby INT,
	birweit INT)
	RETURNS INT AS 
$$
DECLARE

	gestation_age INT;
	sex INT;
	birth_weight INT;
	
	data_quality_score INT;
	sex_baby INT;
	
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
	
	gestation_age := gestat::INT;
	birth_status := birstat::INT;
	sex := sexbaby::INT;
	birth_weight := birweit::INT;
	
	-- Schema values indicate an invalid value in at least one parameter
	IF  birth_status < 1 OR
		birth_status > 4 OR
		sex < 1 OR
		sex > 2 OR
		gestat > 49 THEN
		
		RETURN 3;	
	END IF;

	-- Estimates for reasonable birth weight given a sex and gestational age
	-- at birth are only meaningful for live births.  Therefore, if it isn't
	-- a live birth, then we will assume the weight is valid.
	IF birstat != 1 THEN
		RETURN 8
	END IF;

	/* 
	 * It is possible to have a baby that is 7000g or more but this seems
	 * unlikely.  HES allows it, but we'll assume it is a medically 
	 * infeasible value and return a score of 7.  This is an example of a 
	 * judgement call that would be refined by a domain scientist.
	 */
	IF (birth_weight > 7000) THEN
		RETURN 7
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
	IF sex = 1 AND 
		gestation_age = 24 AND
	   	((birth_weight < 326) OR (birth_weight > 944))) THEN	   	   
		RETURN 2
	ELSIF sex = 1 AND
		gestation_age = 25 AND
		((birth_weight < 379) OR (birth_weight > 1080)) THEN
		RETURN 2
	ELSIF sex = 1 AND
		gestation_age = 26 AND
		((birth_weight < 430) OR (birth_weight > 1207)) THEN
		RETURN 2
	ELSIF sex = 1 AND
		gestation_age = 42 AND
		((birth_weight < 2935) OR (birth_weight > 4748)) THEN
		RETURN 2
	ELSIF sex = 1 AND
		gestation_age = 43 AND
		((birth_weight < 2976) OR (birth_weight > 4781)) THEN
		RETURN 2		
	ELSIF sex = 2 AND 
		gestation_age = 24 AND
	   	((birth_weight < 270) OR (birth_weight > 916))) THEN	   	   
		RETURN 2
	ELSIF sex = 2 AND
		gestation_age = 25 AND
		((birth_weight < 320) OR (birth_weight > 1044)) THEN
		RETURN 2
	ELSIF sex = 2 AND
		gestation_age = 26 AND
		((birth_weight < 382) OR (birth_weight > 1208)) THEN
		RETURN 2
	ELSIF sex = 2 AND
		gestation_age = 42 AND
		((birth_weight < 2935) OR (birth_weight > 4748)) THEN
		RETURN 2
	ELSIF sex = 2 AND
		gestation_age = 43 AND
		((birth_weight < 2909) OR (birth_weight > 4560)) THEN
		RETURN 2
	ELSE 
		RETURN 8
	END IF;

END;





