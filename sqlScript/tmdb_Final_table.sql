alter session set query_tag = '&SF_QUERY_TAG';
!set variable_substitution=false;

INSERT INTO FINAL_TMDB_DATA(
  SELECT
      RELEASE_DATE,
      YEAR(TO_DATE(RELEASE_DATE,'YYYY-MM-DD')) AS YEAR,
      ID,
      ORIGINAL_LANGUAGE AS LANGUAGE,
      TITle,
      POPULARITY,
      VOTE_AVERAGE,
      VOTE_COUNT,
      POPULARITY_RATING
  FROM INTER_TMDB_DATA
 );