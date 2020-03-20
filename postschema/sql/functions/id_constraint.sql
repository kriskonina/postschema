CREATE OR REPLACE FUNCTION identity_constraint_fn() RETURNS TRIGGER
AS $$
DECLARE
	target_table_name       text = quote_ident(TG_ARGV[0]);
	target_table_pk         text = quote_ident(TG_ARGV[1]);
	local_comp_col          text = to_json(NEW) ->> TG_ARGV[2];
	target_col              text = quote_ident(TG_ARGV[3]);
	target_fk_val           int  = to_json(NEW) ->> TG_ARGV[4];
	target_table_local_ref  text = quote_ident(TG_ARGV[4]);
	out character varying(255);
BEGIN
	EXECUTE format('SELECT %%s FROM %%s WHERE %%s=%%s', target_col, target_table_name, target_table_pk, target_fk_val)
	INTO out;
	IF out <> local_comp_col THEN
		RAISE EXCEPTION
        USING ERRCODE='check_violation',
        DETAIL=format('Key (%%s)=(%%s) violates same identity constraint between tables %%s and %%s', target_table_local_ref, target_fk_val, target_table_name, TG_TABLE_NAME);
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;