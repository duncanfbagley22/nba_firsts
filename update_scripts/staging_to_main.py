import psycopg2
from psycopg2.extras import execute_values

def sync_tables():
    conn_params = {
        'dbname': 'nba_data',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': '5432',
    }

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        tables = [
            ("staging_games", "games"),
            ("staging_player_boxscores", "player_boxscores"),
            ("staging_team_boxscores", "team_boxscores")
        ]

        for staging_table, main_table in tables:

            # Get columns in the same order as they appear in the database
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (main_table,))
            main_columns = [row[0] for row in cursor.fetchall()]

            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (staging_table,))
            staging_columns = [row[0] for row in cursor.fetchall()]

            # Ensure columns match between tables
            if main_columns != staging_columns:
                print(f"Column mismatch between {staging_table} and {main_table}. Skipping.")
                continue

            # Fetch data from staging table
            cursor.execute(f"SELECT {', '.join(staging_columns)} FROM {staging_table}")
            staging_records = cursor.fetchall()

            if not staging_records:
                print(f"No records found in {staging_table}")
                continue

            # Check for existing records in the main table
            cursor.execute(f"SELECT {main_columns[0]} FROM {main_table}")
            existing_pks = {row[0] for row in cursor.fetchall()}

            new_records = [record for record in staging_records if record[0] not in existing_pks]

            if new_records:
                print(f"\nFound {len(new_records)} new records to add to {main_table}.")
                for record in new_records[:5]:  # Show a sample of the first 5
                    print(record)
                if len(new_records) > 5:
                    print(f"... and {len(new_records) - 5} more records.")

                # Ask for confirmation
                approval = input(f"Do you want to insert these records into {main_table}? (y/n): ").strip().lower()
                if approval == 'y':
                    # Insert new records
                    insert_query = f"INSERT INTO {main_table} ({', '.join(main_columns)}) VALUES %s"
                    execute_values(cursor, insert_query, new_records)
                    print(f"Inserted {len(new_records)} records into {main_table}.")
                else:
                    print(f"Skipped inserting records into {main_table}.")
            else:
                print(f"No new records to add to {main_table}.")

            # Clear staging table with confirmation
            if input(f"Clear {staging_table}? (y/n): ").strip().lower() == 'y':
                cursor.execute(f"DELETE FROM {staging_table}")
                print(f"Cleared {staging_table}.")
            else:
                print(f"Skipped clearing {staging_table}.")

        conn.commit()
        print("\nData sync completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    sync_tables()
