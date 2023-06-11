alter session set query_tag = '&SF_QUERY_TAG';
!set variable_substitution=false;

INSERT INTO myDb.mySchema.INTER_TMDB_DATA(
	SELECT
    *,
    CASE
        WHEN 300 >= POPULARITY AND POPULARITY > 100 THEN 'Poor'
        WHEN 500 >= POPULARITY AND POPULARITY > 300 THEN 'Below Avg'
        WHEN 700 >= POPULARITY AND POPULARITY > 500 THEN 'Avg'
        WHEN 900 >= POPULARITY AND POPULARITY > 700 THEN 'Good'
        WHEN 1100 >= POPULARITY AND POPULARITY > 900 THEN 'Very Good'
        WHEN 1300 >= POPULARITY AND POPULARITY > 1100 THEN 'Outstanding'
        ELSE 'Excellent'
    END AS POPULARITY_RATING
FROM RAW_TMDB_DATA
);