--DDL for user_devices_cumulated Table
-- Tracks each user’s activity by browser_type day-by-day in a cumulative array
CREATE TABLE user_devices_cumulated(
		user_id TEXT,
		browser_type TEXT,
		device_activity_datelist DATE[],
		date DATE,
		PRIMARY KEY(user_id,browser_type, date)	
);