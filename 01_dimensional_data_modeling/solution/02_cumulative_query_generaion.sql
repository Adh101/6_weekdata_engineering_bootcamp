--Cumulative Query Generation
-- Creating cumulative table query to insert data one year at a time
INSERT INTO actors (
    actorid,
    actor,
    current_year,
    films,
    quality_class,
    is_active
)
WITH years AS (
    SELECT generate_series(1970, 2021) AS current_year
),

actor_base AS (
    SELECT actorid, MAX(actor) AS actor, MIN(year) AS first_year
    FROM actor_films
    GROUP BY actorid
),

actor_years AS (
    SELECT a.actorid, a.actor, y.current_year
    FROM actor_base a
    JOIN years y ON y.current_year >= a.first_year
),

film_years AS (
    SELECT
        ay.actorid,
        ay.current_year,
        MAX(af.year) AS latest_year
    FROM actor_films af
    JOIN actor_years ay ON af.actorid = ay.actorid AND af.year <= ay.current_year
    GROUP BY ay.actorid, ay.current_year
),

films_grouped AS (
    SELECT
        ay.actorid,
        ay.actor,
        ay.current_year,
        ARRAY_AGG(
            ROW(
                af.film,
                af.votes,
                af.rating,
                af.filmid,
                af.year
            )::films_struct
            ORDER BY af.year, af.filmid
        ) AS films
    FROM actor_years ay
    LEFT JOIN actor_films af
        ON af.actorid = ay.actorid AND af.year <= ay.current_year
    GROUP BY ay.actorid, ay.actor, ay.current_year
),

avg_rating_latest_year AS (
    SELECT
        f.actorid,
        f.current_year,
        AVG(af.rating) AS avg_rating
    FROM film_years f
    JOIN actor_films af
        ON af.actorid = f.actorid AND af.year = f.latest_year
    GROUP BY f.actorid, f.current_year
)

SELECT
    f.actorid,
    f.actor,
    f.current_year,
    f.films,
    CASE
        WHEN a.avg_rating > 8 THEN 'star'
        WHEN a.avg_rating > 7 THEN 'good'
        WHEN a.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    EXISTS (
        SELECT 1
        FROM UNNEST(f.films) AS film
        WHERE film.year = f.current_year
    ) AS is_active
FROM films_grouped f
JOIN avg_rating_latest_year a
    ON f.actorid = a.actorid AND f.current_year = a.current_year;