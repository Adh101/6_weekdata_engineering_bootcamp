-- Deduplication query on game_details table
-- Removes duplicate entries by keeping the first one per (game_id, team_id, player_id)
SELECT * FROM game_details;

WITH deduped AS(
		SELECT *,
			ROW_NUMBER() OVER(PARTITION BY game_id,team_id, player_id) AS row_num
		FROM game_details
)
SELECT * FROM deduped
WHERE row_num =1;