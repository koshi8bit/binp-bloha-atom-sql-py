from DB import DB

def main():
    db = DB("10.11.5.91", "UPN276_prm")
    # db.get_table_columns("IEC104Prm_1")
    setup_functions(db)

    find_by_kks(db, "MAG12CE01_XQ01")
    find_by_addr(db, 6839)



def find_by_kks(db, kks):
    db.run_test_short(f"""
SELECT * FROM find_by_kks('{kks}');
        """)

def find_by_addr(db, addr):
    db.run_test_short(f"""
SELECT * FROM find_by_addr('{str(addr)}');
        """)


#     db.run_test("""
# SELECT * FROM \"Archive_val\"
#     """)


def setup_functions(db):
    db.exec("""
    CREATE OR REPLACE FUNCTION find_by_kks(p_kks TEXT)
    RETURNS TABLE (
        table_name TEXT,
        kks TEXT,
        addr TEXT,
        description TEXT
    )
    LANGUAGE plpgsql
    AS $$
    DECLARE
        tbl RECORD;
    BEGIN
        FOR tbl IN
            SELECT table_schema, t.table_name
            FROM information_schema.tables t
            WHERE t.table_name ~* '^IEC104Prm_[0-9]+$'
              AND t.table_type = 'BASE TABLE'
              AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
        LOOP
            RETURN QUERY EXECUTE format(
                'SELECT %L::TEXT,
                        "SHIFR"::TEXT,
                        "Object_Id"::TEXT,
                        "DESCR"::TEXT
                 FROM %I.%I
                 WHERE "SHIFR" = $1',
                tbl.table_schema || '.' || tbl.table_name,
                tbl.table_schema,
                tbl.table_name
            )
            USING p_kks;
        END LOOP;
    END;
    $$;
                """)

    db.exec("""
        CREATE OR REPLACE FUNCTION find_by_addr(p_kks TEXT)
        RETURNS TABLE (
            table_name TEXT,
            kks TEXT,
            addr TEXT,
            description TEXT
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            tbl RECORD;
        BEGIN
            FOR tbl IN
                SELECT table_schema, t.table_name
                FROM information_schema.tables t
                WHERE t.table_name ~* '^IEC104Prm_[0-9]+$'
                  AND t.table_type = 'BASE TABLE'
                  AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
            LOOP
                RETURN QUERY EXECUTE format(
                    'SELECT %L::TEXT,
                            "SHIFR"::TEXT,
                            "Object_Id"::TEXT,
                            "DESCR"::TEXT
                     FROM %I.%I
                     WHERE "Object_Id" = $1::BIGINT',
                    tbl.table_schema || '.' || tbl.table_name,
                    tbl.table_schema,
                    tbl.table_name
                )
                USING p_kks;
            END LOOP;
        END;
        $$;
                    """)

main()
