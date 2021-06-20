Create simple spark batch ETL job that satisfies following points.    
Assume that actual data volume will be several GBs per day   


1. Reads and parses Youtube trending video data from provided JSON files
2. Extracts most viewed video per category id and per trending date
3. Formats data to have required columns for analytics - video_id, trending_date, category_id, title, views, likes, dislikes
4. Save data to partitioned table to be used in further analysis