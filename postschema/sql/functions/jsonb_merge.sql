CREATE OR REPLACE FUNCTION jsonb_merge_deep(jsonb, jsonb)
  RETURNS jsonb
  LANGUAGE SQL
  immutable
AS $func$
SELECT CASE jsonb_typeof($1)
    WHEN 'object' THEN CASE jsonb_typeof($2)
        WHEN 'object' THEN (
            SELECT  jsonb_object_agg(k, CASE
                        WHEN e2.v IS NULL THEN e1.v
                        WHEN e1.v IS NULL THEN e2.v
                        ELSE jsonb_merge_deep(e1.v, e2.v)
                    END)
            FROM  jsonb_each($1) e1(k, v)
            FULL JOIN jsonb_each($2) e2(k, v) USING (k)
        )
        ELSE $2
    END
    WHEN 'array' THEN $1 || $2
    ELSE $2
  END
$func$;