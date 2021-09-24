CREATE TABLE query1 AS
SELECT GENRES.NAME,
	SUBQUERY.MOVIECOUNT
FROM GENRES
LEFT OUTER JOIN 
	(SELECT GENREID,
			COUNT(GENREID) AS MOVIECOUNT
		FROM HASAGENRE
		GROUP BY GENREID) AS SUBQUERY ON GENRES.GENREID = SUBQUERY.GENREID;

CREATE TABLE QUERY2 AS WITH GHR AS
	(SELECT GENRES.NAME,
			HASAGENRE.GENREID,
			RATINGS.MOVIEID,
			RATINGS.RATING
		FROM GENRES
		LEFT OUTER JOIN HASAGENRE ON HASAGENRE.GENREID = GENRES.GENREID
		LEFT OUTER JOIN RATINGS ON RATINGS.MOVIEID = HASAGENRE.MOVIEID)
SELECT NAME,
	AVG(RATING) AS RATING
FROM GHR
GROUP BY NAME
ORDER BY NAME ASC;

CREATE TABLE QUERY3 AS WITH RM AS
	(SELECT RATINGS.RATING,
			RATINGS.MOVIEID,
			MOVIES.TITLE
		FROM RATINGS
		FULL OUTER JOIN MOVIES ON MOVIES.MOVIEID = RATINGS.MOVIEID),
	COUNTER AS
	(SELECT TITLE,
			COUNT(RATING) AS COUNTOFRATINGS
		FROM RM
		GROUP BY TITLE)
SELECT TITLE,
	COUNTOFRATINGS
FROM COUNTER
WHERE COUNTOFRATINGS >= 10;

CREATE TABLE QUERY4 AS WITH GHM AS
	(SELECT GENRES.NAME,
			HASAGENRE.GENREID,
			MOVIES.MOVIEID,
			MOVIES.TITLE
		FROM GENRES
		FULL OUTER JOIN HASAGENRE ON HASAGENRE.GENREID = GENRES.GENREID
		FULL OUTER JOIN MOVIES ON MOVIES.MOVIEID = HASAGENRE.MOVIEID)
SELECT MOVIEID,
	TITLE
FROM GHM
WHERE NAME = 'Comedy';

CREATE TABLE QUERY5 AS WITH RM AS
	(SELECT RATINGS.RATING,
			MOVIES.TITLE
		FROM RATINGS
		LEFT OUTER JOIN MOVIES ON MOVIES.MOVIEID = RATINGS.MOVIEID),
	AVG AS
	(SELECT TITLE,
			AVG(RATING) AS AVERAGE
		FROM RM
		GROUP BY TITLE)
SELECT TITLE,
	AVERAGE
FROM AVG;

CREATE TABLE QUERY6 AS WITH GHMR AS
	(SELECT GENRES.NAME,
	 		RATINGS.RATING
		FROM GENRES
		FULL OUTER JOIN HASAGENRE ON HASAGENRE.GENREID = GENRES.GENREID
		FULL OUTER JOIN MOVIES ON MOVIES.MOVIEID = HASAGENRE.MOVIEID
		FULL OUTER JOIN RATINGS ON MOVIES.MOVIEID = RATINGS.MOVIEID),
	AVG AS
	(SELECT NAME,
	 AVG(RATING) AS AVERAGE
	 FROM GHMR
	 GROUP BY NAME)
SELECT AVERAGE
FROM AVG
WHERE NAME = 'Comedy';

CREATE TABLE QUERY7 AS WITH COMEDY AS
	(SELECT *
		FROM HASAGENRE
		WHERE GENREID = 5 ),
	ROMANCE AS
	(SELECT *
		FROM HASAGENRE
		WHERE GENREID = 14 ),
	COMBINED AS
	(SELECT ROMANCE.MOVIEID,
			ROMANCE.GENREID,
			COMEDY.GENREID
		FROM ROMANCE
		INNER JOIN COMEDY ON ROMANCE.MOVIEID = COMEDY.MOVIEID),
	COMBINEDRAT AS
	(SELECT COMBINED.MOVIEID,
			RATINGS.RATING
		FROM COMBINED
		LEFT OUTER JOIN RATINGS ON COMBINED.MOVIEID = RATINGS.MOVIEID)
SELECT AVG(RATING) AS AVERAGE
FROM COMBINEDRAT;

CREATE TABLE QUERY8 AS WITH ROMANCE AS
	(SELECT DISTINCT MOVIEID,
			GENREID
		FROM HASAGENRE
		WHERE GENREID = 14 ),
	COMEDY AS
	(SELECT DISTINCT MOVIEID,
			GENREID
		FROM HASAGENRE
		WHERE GENREID = 5),
	COMBINE AS
	(SELECT R.MOVIEID
		FROM ROMANCE AS R
		LEFT JOIN COMEDY AS C ON R.MOVIEID = C.MOVIEID
		WHERE C.MOVIEID IS NULL),
	COMBINER AS
	(SELECT COMBINE.MOVIEID,
			RATINGS.RATING
		FROM RATINGS
		INNER JOIN COMBINE ON RATINGS.MOVIEID = COMBINE.MOVIEID)
SELECT AVG(RATING) AS AVERAGE
FROM COMBINER;

DECLARE V1 integer;


CREATE TABLE QUERY9 AS
SELECT MOVIEID,
	RATING
FROM RATINGS
WHERE RATINGS.USERID = :v1;

CREATE TABLE RECOMMENDATION AS WITH RM2 AS
	(SELECT RATINGS.RATING,
			MOVIES.MOVIEID
		FROM RATINGS
		LEFT OUTER JOIN MOVIES ON MOVIES.MOVIEID = RATINGS.MOVIEID),
	AVG1 AS
	(SELECT MOVIEID,
			AVG(RATING) AS AVERAGE
		FROM RM2
		GROUP BY MOVIEID),
	AVG2 AS
	(SELECT MOVIEID,
			AVG(RATING) AS AVERAGE
		FROM RM2
		GROUP BY MOVIEID),
	SIM AS
	(SELECT AVG1.MOVIEID AS MOVIEID1,
			AVG2.MOVIEID AS MOVIEID2
		FROM AVG1
		CROSS JOIN AVG2),
	SIM2 AS
	(SELECT SIM.MOVIEID1 AS MOVIEID1,
			SIM.MOVIEID2 AS MOVIEID2,
			AVG1.AVERAGE AS AVG1
		FROM SIM
		LEFT OUTER JOIN AVG1 ON SIM.MOVIEID1 = AVG1.MOVIEID),
	SIM3 AS
	(SELECT SIM2.MOVIEID1 AS MOVIEID1,
			SIM2.MOVIEID2 AS MOVIEID2,
			SIM2.AVG1 AS AVG1,
			AVG2.AVERAGE AS AVG2,
			ABS(1 - ((SIM2.AVG1 - AVG2.AVERAGE) / 5)) AS SIM
		FROM SIM2
		LEFT OUTER JOIN AVG2 ON SIM2.MOVIEID2 = AVG2.MOVIEID),
	SIMILARITY AS
	(SELECT MOVIEID1,
			MOVIEID2,
			SIM
		FROM SIM3),
	T1 AS
	(SELECT MOVIEID,
			RATING
		FROM RATINGS
		WHERE USERID = V1:),
	T2 AS
	(SELECT SIMILARITY.MOVIEID1 AS NOT_RATED,
			SIMILARITY.MOVIEID2 AS RATED,
			SIMILARITY.SIM AS SIM,
			T1.RATING AS RATING
		FROM SIMILARITY
		LEFT OUTER JOIN T1 ON SIMILARITY.MOVIEID2 = T1.MOVIEID),
	T3 AS
	(SELECT T2.NOT_RATED,
			T2.RATED,
			T2.SIM,
			T2.RATING,
			T2.RATING * T2.SIM AS TEST1
		FROM T2
		WHERE RATING IS NOT NULL),
	T4 AS
	(SELECT T3.NOT_RATED,
			SUM(T3.TEST1) AS SUMTEST1,
			SUM(T3.SIM) AS SUMSIM
		FROM T3
		GROUP BY NOT_RATED),
	T5 AS
	(SELECT T4.NOT_RATED,
			T4.SUMTEST1 / T4.SUMSIM AS PRED
		FROM T4),
	T6 AS
	(SELECT MOVIES.TITLE AS TITLE,
			T5.PRED
		FROM T5
		LEFT OUTER JOIN MOVIES ON T5.NOT_RATED = MOVIES.MOVIEID)
SELECT TITLE
FROM T6
WHERE PRED > 3.9;