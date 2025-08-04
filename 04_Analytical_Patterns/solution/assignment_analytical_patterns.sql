-- Week 4 Assignment on Analytical Patterns

--A query that does state change tracking for players
WITH player_state AS(
	SELECT
		player_name,
		current_season,
		is_active,
		LAG(is_active) OVER(PARTITION BY player_name ORDER BY current_season) AS prev_active -- Looks at the previous years is_active status
	FROM players
)

SELECT
	player_name,
	current_season,
	is_active,
	-- Case to track the players change state
	CASE
		WHEN prev_active IS NULL AND is_active = TRUE THEN 'NEW'
		WHEN prev_active = TRUE AND is_active = TRUE THEN 'CONTINUED PLAYING'
		WHEN prev_active = TRUE AND is_active = FALSE THEN 'RETIRED'
		WHEN prev_active = FALSE AND is_active = TRUE THEN 'RETURNED FROM RETIREMENT'
		WHEN prev_active = FALSE AND is_active = FALSE THEN 'STAYED RETIRED'
		ELSE 'UNKNOWN'
	END AS active_status
FROM player_state;

-- A query that uses GROUPING SETS to do efficient aggregations of game_details data
WITH player_team_season_stats AS (
    SELECT
        gd.player_name,
        gd.team_abbreviation,
        ps.season,
        gd.pts,
        NULL::INT AS total_wins
    FROM game_details gd
    JOIN player_seasons ps
      ON gd.player_name = ps.player_name
),

game_winners AS (
    SELECT
        CASE
            WHEN pts_home > pts_away THEN team_id_home
            ELSE team_id_away
        END AS winning_team_id
    FROM games
),

team_win_counts AS (
    SELECT
        winning_team_id AS team_id,
        COUNT(*) AS total_wins
    FROM game_winners
    GROUP BY winning_team_id
),

team_abbreviations AS (
    SELECT DISTINCT team_id, team_abbreviation
    FROM game_details
),

team_wins_named AS (
    SELECT
        NULL::TEXT AS player_name,                    -- same as player_name
        t.team_abbreviation,
        NULL::INTEGER AS season,                      -- cast to match season INT
        NULL::INT AS pts,                             -- match pts INT
        w.total_wins
    FROM team_win_counts w
    JOIN team_abbreviations t
      ON t.team_id = w.team_id
),

combined_stats AS (
    SELECT * FROM player_team_season_stats
    UNION ALL
    SELECT * FROM team_wins_named
),

aggregated_labeled_stats AS (
    SELECT
        CASE
            WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'player__team'
            WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player__season'
            WHEN GROUPING(team_abbreviation) = 0 THEN 'team'
            ELSE 'overall'
        END AS aggregation_level,
        COALESCE(player_name, '(overall)') AS player_name,
        COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
        COALESCE(MIN(season)::TEXT, '(overall)') AS season,
        SUM(pts) AS total_points,
        SUM(total_wins) AS total_wins
    FROM combined_stats
    GROUP BY GROUPING SETS (
        (player_name, team_abbreviation), -- Points scored for one team
        (player_name, season),            -- Points scored in a season
        (team_abbreviation)               -- Games won by each team
    )
)

SELECT *
FROM aggregated_labeled_stats
ORDER BY aggregation_level, total_points DESC NULLS LAST, total_wins DESC NULLS LAST;



-- Query to identify the player who scored the most points for a single team.
WITH player_team_season_stats AS (
    SELECT
        gd.player_name,
        gd.team_abbreviation,
        ps.season,
        gd.pts,
        NULL::INT AS total_wins
    FROM game_details gd
    JOIN player_seasons ps
      ON gd.player_name = ps.player_name
),

game_winners AS (
    SELECT
        CASE
            WHEN pts_home > pts_away THEN team_id_home
            ELSE team_id_away
        END AS winning_team_id
    FROM games
),

team_win_counts AS (
    SELECT
        winning_team_id AS team_id,
        COUNT(*) AS total_wins
    FROM game_winners
    GROUP BY winning_team_id
),

team_abbreviations AS (
    SELECT DISTINCT team_id, team_abbreviation
    FROM game_details
),

team_wins_named AS (
    SELECT
        NULL::TEXT AS player_name,                    -- same as player_name
        t.team_abbreviation,
        NULL::INTEGER AS season,                      -- cast to match season INT
        NULL::INT AS pts,                             -- match pts INT
        w.total_wins
    FROM team_win_counts w
    JOIN team_abbreviations t
      ON t.team_id = w.team_id
),

combined_stats AS (
    SELECT * FROM player_team_season_stats
    UNION ALL
    SELECT * FROM team_wins_named
),

aggregated_labeled_stats AS (
    SELECT
        CASE
            WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'player__team'
            WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player__season'
            WHEN GROUPING(team_abbreviation) = 0 THEN 'team'
            ELSE 'overall'
        END AS aggregation_level,
        COALESCE(player_name, '(overall)') AS player_name,
        COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
        COALESCE(MIN(season)::TEXT, '(overall)') AS season,
        SUM(pts) AS total_points,
        SUM(total_wins) AS total_wins
    FROM combined_stats
    GROUP BY GROUPING SETS (
        (player_name, team_abbreviation), -- Points scored for one team
        (player_name, season),            -- Points scored in a season
        (team_abbreviation)               -- Games won by each team
    )
)

SELECT *
FROM aggregated_labeled_stats
WHERE aggregation_level = 'player__team'
ORDER BY aggregation_level, total_points DESC NULLS LAST, total_wins DESC NULLS LAST;

-- Query to identify the player who scored the most points in a single season.
WITH player_team_season_stats AS (
    SELECT
        gd.player_name,
        gd.team_abbreviation,
        ps.season,
        gd.pts,
        NULL::INT AS total_wins
    FROM game_details gd
    JOIN player_seasons ps
      ON gd.player_name = ps.player_name
),

game_winners AS (
    SELECT
        CASE
            WHEN pts_home > pts_away THEN team_id_home
            ELSE team_id_away
        END AS winning_team_id
    FROM games
),

team_win_counts AS (
    SELECT
        winning_team_id AS team_id,
        COUNT(*) AS total_wins
    FROM game_winners
    GROUP BY winning_team_id
),

team_abbreviations AS (
    SELECT DISTINCT team_id, team_abbreviation
    FROM game_details
),

team_wins_named AS (
    SELECT
        NULL::TEXT AS player_name,                    -- same as player_name
        t.team_abbreviation,
        NULL::INTEGER AS season,                      -- cast to match season INT
        NULL::INT AS pts,                             -- match pts INT
        w.total_wins
    FROM team_win_counts w
    JOIN team_abbreviations t
      ON t.team_id = w.team_id
),

combined_stats AS (
    SELECT * FROM player_team_season_stats
    UNION ALL
    SELECT * FROM team_wins_named
),

aggregated_labeled_stats AS (
    SELECT
        CASE
            WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'player__team'
            WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player__season'
            WHEN GROUPING(team_abbreviation) = 0 THEN 'team'
            ELSE 'overall'
        END AS aggregation_level,
        COALESCE(player_name, '(overall)') AS player_name,
        COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
        COALESCE(MIN(season)::TEXT, '(overall)') AS season,
        SUM(pts) AS total_points,
        SUM(total_wins) AS total_wins
    FROM combined_stats
    GROUP BY GROUPING SETS (
        (player_name, team_abbreviation), -- Points scored for one team
        (player_name, season),            -- Points scored in a season
        (team_abbreviation)               -- Games won by each team
    )
)

SELECT *
FROM aggregated_labeled_stats
WHERE aggregation_level = 'player__season'
ORDER BY aggregation_level, total_points DESC NULLS LAST, total_wins DESC NULLS LAST;

-- -- Query to identify the team with the most total wins.
WITH player_team_season_stats AS (
    SELECT
        gd.player_name,
        gd.team_abbreviation,
        ps.season,
        gd.pts,
        NULL::INT AS total_wins
    FROM game_details gd
    JOIN player_seasons ps
      ON gd.player_name = ps.player_name
),

game_winners AS (
    SELECT
        CASE
            WHEN pts_home > pts_away THEN team_id_home
            ELSE team_id_away
        END AS winning_team_id
    FROM games
),

team_win_counts AS (
    SELECT
        winning_team_id AS team_id,
        COUNT(*) AS total_wins
    FROM game_winners
    GROUP BY winning_team_id
),

team_abbreviations AS (
    SELECT DISTINCT team_id, team_abbreviation
    FROM game_details
),

team_wins_named AS (
    SELECT
        NULL::TEXT AS player_name,                    -- same as player_name
        t.team_abbreviation,
        NULL::INTEGER AS season,                      -- cast to match season INT
        NULL::INT AS pts,                             -- match pts INT
        w.total_wins
    FROM team_win_counts w
    JOIN team_abbreviations t
      ON t.team_id = w.team_id
),

combined_stats AS (
    SELECT * FROM player_team_season_stats
    UNION ALL
    SELECT * FROM team_wins_named
),

aggregated_labeled_stats AS (
    SELECT
        CASE
            WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'player__team'
            WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player__season'
            WHEN GROUPING(team_abbreviation) = 0 THEN 'team'
            ELSE 'overall'
        END AS aggregation_level,
        COALESCE(player_name, '(overall)') AS player_name,
        COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
        COALESCE(MIN(season)::TEXT, '(overall)') AS season,
        SUM(pts) AS total_points,
        SUM(total_wins) AS total_wins
    FROM combined_stats
    GROUP BY GROUPING SETS (
        (player_name, team_abbreviation), -- Points scored for one team
        (player_name, season),            -- Points scored in a season
        (team_abbreviation)               -- Games won by each team
    )
)

SELECT *
FROM aggregated_labeled_stats
WHERE aggregation_level = 'team'
ORDER BY aggregation_level, total_points DESC NULLS LAST, total_wins DESC NULLS LAST;




--A query that uses window functions on game_details
-- What is the most games a team has won in a 90 game stretch?
WITH team_scores AS (
    SELECT
        game_id,
        team_id,
        team_abbreviation,
        SUM(pts) AS team_points
    FROM game_details
    GROUP BY game_id, team_id, team_abbreviation
),
ranked_teams AS (
    SELECT
        game_id,
        team_id,
        team_abbreviation,
        team_points,
        RANK() OVER (PARTITION BY game_id ORDER BY team_points DESC) AS rank
    FROM team_scores
),
winning_games AS (
    SELECT
        team_id,
        team_abbreviation,
        game_id,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_id) AS game_num
    FROM ranked_teams
    WHERE rank = 1  -- Top scoring team = winner
),
team_win_streaks AS (
    SELECT
        team_id,
        team_abbreviation,
        game_num,
        COUNT(*) OVER (
            PARTITION BY team_id
            ORDER BY game_num
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90_game_stretch
    FROM winning_games
)

SELECT *
FROM team_win_streaks
ORDER BY wins_in_90_game_stretch DESC;

-- How many games in a row did LeBron James score over 10 points a game?
WITH lebron_games AS (
    SELECT
        game_id,
        player_name,
        pts,
        CASE WHEN pts > 10 THEN 1 ELSE 0 END AS over_10_flag
    FROM game_details
    WHERE player_name = 'LeBron James'
),
streaked AS (
    SELECT *,
           SUM(CASE WHEN over_10_flag = 0 THEN 1 ELSE 0 END) OVER (
               ORDER BY game_id
               ROWS UNBOUNDED PRECEDING
           ) AS streak_breaker
    FROM lebron_games
),
grouped_streaks AS (
    SELECT
        streak_breaker,
        COUNT(*) AS streak_length
    FROM streaked
    WHERE over_10_flag = 1
    GROUP BY streak_breaker
)

SELECT MAX(streak_length) AS longest_over_10_streak
FROM grouped_streaks;











