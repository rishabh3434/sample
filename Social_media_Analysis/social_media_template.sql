/* =====================================================================
   SOCIAL MEDIA (IG CLONE) — STUDENT ANSWER TEMPLATE
   ===================================================================== */

USE ig_clone;


/* =====================================================================
   OBJECTIVE QUESTIONS
   ===================================================================== */

-- O1.
-- Are there any tables with duplicate or missing null values? If so, how
-- would you handle them?
SELECT
    id,
    username,
    created_at
FROM users
WHERE username IS NULL;

SELECT
    username,
    COUNT(*) AS count
FROM users
GROUP BY username
HAVING COUNT(*) > 1;
   
SELECT
    id,
    image_url,
    user_id,
    created_dat
FROM photos
WHERE image_url IS NULL
   OR user_id IS NULL
   OR created_dat IS NULL;

SELECT
    image_url,
    user_id,
    COUNT(*) AS count
FROM photos
GROUP BY image_url, user_id
HAVING COUNT(*) > 1;

SELECT
    id,
    comment_text,
    photo_id,
    user_id,
    created_at
FROM comments
WHERE comment_text IS NULL
   OR photo_id IS NULL
   OR user_id IS NULL
   OR created_at IS NULL;
   
SELECT
    comment_text,
    user_id,
    photo_id,
    COUNT(*) AS count
FROM comments
GROUP BY comment_text,user_id,photo_id
HAVING COUNT(*) > 1;

SELECT
    user_id,
    photo_id,
    created_at
FROM likes
WHERE user_id IS NULL
   OR photo_id IS NULL
   OR created_at IS NULL;
   
SELECT
    user_id,
    photo_id,
    COUNT(*) AS count
FROM likes
GROUP BY user_id,photo_id
HAVING COUNT(*) > 1;

SELECT
    follower_id,
    followee_id,
    created_at
FROM follows
WHERE follower_id IS NULL
   OR followee_id IS NULL
   OR created_at IS NULL;
   
SELECT
    follower_id,
    followee_id,
    COUNT(*) AS count
FROM follows
GROUP BY follower_id,followee_id
HAVING COUNT(*) > 1;



-- O2.
-- What is the distribution of user activity levels (e.g., number of posts,
-- likes, comments) across the user base?
SELECT
    u.id AS user_id,
    u.username,
    COUNT(DISTINCT p.id) AS total_posts,
    COUNT(DISTINCT l.photo_id) AS total_likes_given,
    COUNT(DISTINCT c.id) AS total_comments_made
FROM users u
LEFT JOIN photos p
    ON u.id = p.user_id
LEFT JOIN likes l
    ON u.id = l.user_id
LEFT JOIN comments c
    ON u.id = c.user_id
GROUP BY u.id,u.username;



-- O3.
-- Calculate the average number of tags per post (photo_tags and photos
-- tables).
SELECT
    ROUND(
        COUNT(pt.tag_id) * 1.0 /
        COUNT(DISTINCT p.id),
        2
    ) AS AverageTags
FROM photos p
LEFT JOIN photo_tags pt
    ON p.id = pt.photo_id;


-- O4.
-- Identify the top users with the highest engagement rates (likes, comments)
-- on their posts and rank them.
SELECT
    p.user_id,
    COUNT(DISTINCT l.user_id) +
    COUNT(DISTINCT c.id) AS Total,
    DENSE_RANK() OVER(
        ORDER BY
        COUNT(DISTINCT l.user_id) +
        COUNT(DISTINCT c.id) DESC
    ) AS rnk
FROM photos p
INNER JOIN likes l
    ON p.id = l.photo_id
INNER JOIN comments c
    ON p.id = c.photo_id
GROUP BY p.user_id;


-- O5.
-- Which users have the highest number of followers and followings?
SELECT
    u.id AS user_id,
    u.username,
    COUNT(DISTINCT f1.follower_id) AS followers,
    COUNT(DISTINCT f2.followee_id) AS following
FROM users u
LEFT JOIN follows f1
    ON u.id = f1.followee_id
LEFT JOIN follows f2
    ON u.id = f2.follower_id
GROUP BY u.id,u.username;


-- O6.
-- Calculate the average engagement rate (likes, comments) per post for each
-- user.
SELECT
    u.id,
    u.username,
    ROUND(
        (COUNT(DISTINCT l.user_id) +COUNT(DISTINCT c.id)) /
        COUNT(DISTINCT p.id),2) AS avg_engagement
FROM users u
JOIN photos p
    ON u.id = p.user_id
LEFT JOIN likes l
    ON p.id = l.photo_id
LEFT JOIN comments c
    ON p.id = c.photo_id
GROUP BY u.id,u.username;


-- O7.
-- Get the list of users who have never liked any post (users and likes
-- tables)
SELECT
    u.id,
    u.username
FROM users u
LEFT JOIN likes l
    ON u.id = l.user_id
WHERE l.user_id IS NULL;


-- O8.
-- How can you leverage user-generated content (posts, hashtags, photo tags)
-- to create more personalized and engaging ad campaigns?
SELECT
    u.id AS user_id,
    u.username,
    t.tag_name,
    COUNT(*) AS tag_usage,
    COUNT(DISTINCT l.user_id) AS total_likes_received,
    COUNT(DISTINCT c.id) AS total_comments_received,
    COUNT(DISTINCT l.user_id)
      + COUNT(DISTINCT c.id) AS total_engagement
FROM users u JOIN photos p ON u.id = p.user_id
JOIN photo_tags pt ON p.id = pt.photo_id
JOIN tags t ON pt.tag_id = t.id
LEFT JOIN likes l ON p.id = l.photo_id
LEFT JOIN comments c ON p.id = c.photo_id
GROUP BY 1,2,3;


-- O9.
-- Are there any correlations between user activity levels and specific
-- content types (e.g., photos, videos, reels)? How can this information
-- guide content creation and curation strategies?
SELECT
    p.user_id,
    COUNT(DISTINCT p.id) AS total_posts,
    COUNT(DISTINCT l.user_id) AS total_likes,
    COUNT(DISTINCT c.id) AS total_comments,
    COUNT(DISTINCT l.user_id)
      + COUNT(DISTINCT c.id) AS total_engagement
FROM photos p
LEFT JOIN likes l
    ON p.id = l.photo_id
LEFT JOIN comments c
    ON p.id = c.photo_id
GROUP BY p.user_id;


-- O10.
-- Calculate the total number of likes, comments, and photo tags for each
-- user.
SELECT
    u.id AS user_id,
    u.username,
    COUNT(DISTINCT l.photo_id) AS total_likes,
    COUNT(DISTINCT c.id) AS total_comments,
    COUNT(DISTINCT pt.tag_id) AS total_tags
FROM users u
LEFT JOIN likes l
      ON u.id = l.user_id
LEFT JOIN comments c
      ON u.id = c.user_id
LEFT JOIN photos p
      ON u.id = p.user_id
LEFT JOIN photo_tags pt
      ON p.id = pt.photo_id
GROUP BY u.id,u.username;


-- O11.
-- Rank users based on their total engagement (likes, comments, shares) over a
-- month.
SELECT
    u.id AS user_id,
    u.username,
    COUNT(DISTINCT l.photo_id) AS likes_given,
    COUNT(DISTINCT c.id) AS comments_made,
    COUNT(DISTINCT l.photo_id)
     + COUNT(DISTINCT c.id) AS total_engagement,
    DENSE_RANK() OVER(
        ORDER BY
        COUNT(DISTINCT l.photo_id)
        + COUNT(DISTINCT c.id) DESC
    ) AS engagement_rank
FROM users u
LEFT JOIN likes l
      ON u.id=l.user_id
LEFT JOIN comments c
      ON u.id=c.user_id
GROUP BY u.id,u.username;


-- O12.
-- Retrieve the hashtags that have been used in posts with the highest average
-- number of likes. Use a CTE to calculate the average likes for each
-- hashtag first.
WITH hashtag_stats AS
(
    SELECT
        t.id AS tag_id,
        t.tag_name,
        COUNT(l.user_id)*1.0 / COUNT(DISTINCT pt.photo_id) AS avg_likes_per_photo,
        COUNT(l.user_id) AS total_likes,
        COUNT(DISTINCT pt.photo_id) AS total_photos
    FROM tags t
    JOIN photo_tags pt ON t.id = pt.tag_id
    LEFT JOIN likes l ON pt.photo_id = l.photo_id
    GROUP BY t.id,t.tag_name
)
SELECT *
FROM hashtag_stats
ORDER BY avg_likes_per_photo DESC
LIMIT 10;


-- O13.
-- Retrieve the users who have started following someone after being followed
-- by that person.
SELECT
    distinct f1.follower_id
FROM follows f1
JOIN follows f2
  ON f1.follower_id = f2.followee_id
 AND f1.followee_id = f2.follower_id
 WHERE f1.created_at >= f2.created_at;


/* =====================================================================
   SUBJECTIVE QUESTIONS
   ===================================================================== */

-- S1.
-- Based on user engagement and activity levels, which users would you
-- consider the most loyal or valuable? How would you reward or incentivize
-- these users?
WITH posts AS (
    SELECT user_id, COUNT(*) AS total_posts
    FROM photos GROUP BY user_id
),
likes_given AS (
    SELECT user_id, COUNT(*) AS likes_given FROM likes GROUP BY user_id
),
comments_made AS (
    SELECT user_id, COUNT(*) AS comments_made FROM comments GROUP BY user_id
),
likes_received AS (
    SELECT p.user_id, COUNT(*) AS likes_received FROM photos p JOIN likes l ON p.id = l.photo_id
    GROUP BY p.user_id
),
comments_received AS (
    SELECT p.user_id, COUNT(*) AS comments_received
    FROM photos p JOIN comments c ON p.id = c.photo_id
    GROUP BY p.user_id
)

SELECT
    u.id,
    u.username,
    COALESCE(po.total_posts,0) AS total_posts,
    COALESCE(lg.likes_given,0) AS likes_given,
    COALESCE(cm.comments_made,0) AS comments_made,
    COALESCE(lr.likes_received,0) AS likes_received,
    COALESCE(cr.comments_received,0) AS comments_received,

    (
        COALESCE(po.total_posts,0)
        + COALESCE(lg.likes_given,0)
        + COALESCE(cm.comments_made,0)
        + COALESCE(lr.likes_received,0)
        + COALESCE(cr.comments_received,0)
    ) AS loyalty_score,

    DENSE_RANK() OVER(
        ORDER BY
        (
            COALESCE(po.total_posts,0)
            + COALESCE(lg.likes_given,0)
            + COALESCE(cm.comments_made,0)
            + COALESCE(lr.likes_received,0)
            + COALESCE(cr.comments_received,0)
        ) DESC
    ) AS rank_num

FROM users u
LEFT JOIN posts po ON u.id = po.user_id
LEFT JOIN likes_given lg ON u.id = lg.user_id
LEFT JOIN comments_made cm ON u.id = cm.user_id
LEFT JOIN likes_received lr ON u.id = lr.user_id
LEFT JOIN comments_received cr ON u.id = cr.user_id
ORDER BY loyalty_score DESC;


-- S2.
-- For inactive users, what strategies would you recommend to re-engage them
-- and encourage them to start posting or engaging again?
SELECT
    u.id,
    u.username
FROM users u
LEFT JOIN photos p ON u.id = p.user_id
LEFT JOIN likes l ON u.id = l.user_id
LEFT JOIN comments c ON u.id = c.user_id
GROUP BY 1,2
HAVING COUNT(DISTINCT p.id) = 0
AND COUNT(DISTINCT l.photo_id) = 0
AND COUNT(DISTINCT c.id) = 0;


-- S3.
-- Which hashtags or content topics have the highest engagement rates? How can
-- this information guide content strategy and ad campaigns?
SELECT
    t.id AS tag_id,
    t.tag_name,
    COUNT(DISTINCT l.user_id) AS total_likes,
    COUNT(DISTINCT c.id) AS total_comments,
    COUNT(DISTINCT l.user_id) + COUNT(DISTINCT c.id) AS total_engagement,
    ROUND(
        (COUNT(DISTINCT l.user_id) + COUNT(DISTINCT c.id))
        / COUNT(DISTINCT pt.photo_id),
        2
    ) AS engagement_rate
FROM tags t
JOIN photo_tags pt ON t.id = pt.tag_id
JOIN photos p ON pt.photo_id = p.id
LEFT JOIN likes l ON p.id = l.photo_id
LEFT JOIN comments c ON p.id = c.photo_id
GROUP BY 1,2
ORDER BY engagement_rate DESC;

-- S4.
-- Are there any patterns or trends in user engagement based on demographics
-- (age, location, gender) or posting times? How can these insights inform
-- targeted marketing campaigns?
SELECT
    HOUR(p.created_dat) AS posting_hour,
    COUNT(DISTINCT p.id) AS total_posts,
    COUNT(DISTINCT l.user_id) AS total_likes,
    COUNT(DISTINCT c.id) AS total_comments,
    COUNT(DISTINCT l.user_id) + COUNT(DISTINCT c.id) AS total_engagement
FROM photos p
LEFT JOIN likes l
    ON p.id = l.photo_id
LEFT JOIN comments c
    ON p.id = c.photo_id
GROUP BY HOUR(p.created_dat)
ORDER BY total_engagement DESC;

-- S5.
-- Based on follower counts and engagement rates, which users would be ideal
-- candidates for influencer marketing campaigns? How would you approach
-- and collaborate with these influencers?
WITH followers AS (
    SELECT followee_id AS user_id, COUNT(*) AS followers
    FROM follows
    GROUP BY followee_id
),
posts AS (
    SELECT user_id, COUNT(*) AS total_posts
    FROM photos
    GROUP BY user_id
),
likes_received AS (
    SELECT p.user_id, COUNT(*) AS likes_received
    FROM photos p
    JOIN likes l ON p.id = l.photo_id
    GROUP BY p.user_id
),
comments_received AS (
    SELECT p.user_id, COUNT(*) AS comments_received
    FROM photos p
    JOIN comments c ON p.id = c.photo_id
    GROUP BY p.user_id
)

SELECT u.id AS user_id,
    u.username,
    COALESCE(f.followers,0) AS followers,
    COALESCE(po.total_posts,0) AS total_posts,
    COALESCE(lr.likes_received,0) AS likes_received,
    COALESCE(cr.comments_received,0) AS comments_received,
    ROUND(
        (
            COALESCE(lr.likes_received,0) + COALESCE(cr.comments_received,0)
        ) /NULLIF(COALESCE(po.total_posts,0),0),2) AS engagement_rate,
    DENSE_RANK() OVER(
        ORDER BY COALESCE(f.followers,0) DESC,
            ROUND((
                    COALESCE(lr.likes_received,0) + COALESCE(cr.comments_received,0)
                ) / NULLIF(COALESCE(po.total_posts,0),0),2) DESC) AS influencer_rank
FROM users u
LEFT JOIN followers f ON u.id = f.user_id
LEFT JOIN posts po ON u.id = po.user_id
LEFT JOIN likes_received lr ON u.id = lr.user_id
LEFT JOIN comments_received cr ON u.id = cr.user_id
WHERE COALESCE(po.total_posts,0) > 0
ORDER BY influencer_rank;


-- S6.
-- Based on user behavior and engagement data, how would you segment the user
-- base for targeted marketing campaigns or personalized recommendations?
WITH posts AS (
    SELECT user_id, COUNT(*) AS total_posts
    FROM photos
    GROUP BY user_id
),
likes_given AS (
    SELECT user_id, COUNT(*) AS likes_given
    FROM likes
    GROUP BY user_id
),
comments_made AS (
    SELECT user_id, COUNT(*) AS comments_made
    FROM comments
    GROUP BY user_id
),
likes_received AS (
    SELECT p.user_id, COUNT(*) AS likes_received
    FROM photos p
    JOIN likes l ON p.id = l.photo_id
    GROUP BY p.user_id
),
comments_received AS (
    SELECT p.user_id, COUNT(*) AS comments_received
    FROM photos p
    JOIN comments c ON p.id = c.photo_id
    GROUP BY p.user_id
)

SELECT u.id,
    u.username,
    COALESCE(po.total_posts,0) AS total_posts,
    COALESCE(lg.likes_given,0) AS likes_given,
    COALESCE(cm.comments_made,0) AS comments_made,
    COALESCE(lr.likes_received,0) AS likes_received,
    COALESCE(cr.comments_received,0) AS comments_received,
    CASE
        WHEN COALESCE(po.total_posts,0) >= 5 AND (COALESCE(lr.likes_received,0) + COALESCE(cr.comments_received,0)) >= 20 THEN 'Power User'
        WHEN COALESCE(po.total_posts,0) >= 1 AND (COALESCE(lr.likes_received,0) + COALESCE(cr.comments_received,0)) >= 10 THEN 'Content Creator'
        WHEN COALESCE(lg.likes_given,0) >= 10 OR COALESCE(cm.comments_made,0) >= 5 THEN 'Highly Engaged User'
        WHEN COALESCE(po.total_posts,0) = 0 AND COALESCE(lg.likes_given,0) = 0 AND COALESCE(cm.comments_made,0) = 0 THEN 'Inactive User'
        ELSE 'Casual User'
    END AS user_segment
FROM users u
LEFT JOIN posts po ON u.id = po.user_id
LEFT JOIN likes_given lg ON u.id = lg.user_id
LEFT JOIN comments_made cm ON u.id = cm.user_id
LEFT JOIN likes_received lr ON u.id = lr.user_id
LEFT JOIN comments_received cr ON u.id = cr.user_id;


-- S7.
-- If data on ad campaigns (impressions, clicks, conversions) is available,
-- how would you measure their effectiveness and optimize future campaigns?
SELECT
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    conversions,
    ROUND((clicks * 100.0) / NULLIF(impressions,0), 2) AS ctr,
    ROUND((conversions * 100.0) / NULLIF(clicks,0), 2) AS conversion_rate,
    ROUND((conversions * 100.0) / NULLIF(impressions,0), 2) AS overall_conversion_rate
FROM Ad_Campaigns
ORDER BY conversion_rate DESC;

-- S8.
-- How can you use user activity data to identify potential brand ambassadors
-- or advocates who could help promote Instagram's initiatives or events?
WITH followers AS (
    SELECT followee_id AS user_id, COUNT(*) AS followers
    FROM follows
    GROUP BY followee_id
),
posts AS (
    SELECT user_id, COUNT(*) AS total_posts
    FROM photos
    GROUP BY user_id
),
likes_received AS (
    SELECT p.user_id, COUNT(*) AS likes_received
    FROM photos p
    JOIN likes l ON p.id = l.photo_id
    GROUP BY p.user_id
),
comments_received AS (
    SELECT p.user_id, COUNT(*) AS comments_received
    FROM photos p
    JOIN comments c ON p.id = c.photo_id
    GROUP BY p.user_id
)

SELECT
    u.id,
    u.username,
    COALESCE(f.followers,0) AS followers,
    COALESCE(po.total_posts,0) AS total_posts,
    COALESCE(lr.likes_received,0) AS likes_received,
    COALESCE(cr.comments_received,0) AS comments_received,
    (
        COALESCE(f.followers,0) +
        COALESCE(po.total_posts,0) +
        COALESCE(lr.likes_received,0) +
        COALESCE(cr.comments_received,0)
    ) AS ambassador_score,
    DENSE_RANK() OVER(
        ORDER BY (
            COALESCE(f.followers,0) +
            COALESCE(po.total_posts,0) +
            COALESCE(lr.likes_received,0) +
            COALESCE(cr.comments_received,0)
        ) DESC
    ) AS ambassador_rank
FROM users u
LEFT JOIN followers f ON u.id = f.user_id
LEFT JOIN posts po ON u.id = po.user_id
LEFT JOIN likes_received lr ON u.id = lr.user_id
LEFT JOIN comments_received cr ON u.id = cr.user_id
ORDER BY ambassador_rank;

-- S9.
-- How would you approach this problem, if the objective and subjective
-- questions weren't given?
-- by agregating data on username and tags to analyze activity and reach


-- S10.
-- Assuming there's a "User_Interactions" table tracking user engagements, how
-- can you update the "Engagement_Type" column to change all instances of
-- "Like" to "Heart" to align with Instagram's terminology?
UPDATE User_Interactions
SET Engagement_Type = 'Heart'
WHERE Engagement_Type = 'Like';


/* ===================================== END OF FILE ===================================== */
