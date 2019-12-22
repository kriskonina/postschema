WITH workspace_cte AS (
    SELECT members AS mems 
    FROM workspace 
    WHERE id = {workspace}
),
actor_cte AS (
    SELECT json_build_object({selects}) AS js, 
           count(*) OVER() AS full_count
    FROM actor, workspace_cte
    WHERE workspace_cte.mems @> actor.id::text::jsonb AND {where}
    ORDER BY {orderby} {orderhow}
    LIMIT {limit}
    OFFSET {offset}
)
SELECT json_build_object(
    'data', json_agg(actor_cte.js), 
    'total_count', actor_cte.full_count
)
FROM actor_cte
GROUP BY actor_cte.full_count