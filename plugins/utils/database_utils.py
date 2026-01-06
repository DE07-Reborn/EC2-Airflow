import psycopg2
import os

class Database_utils:
    """
        Connect to PostgresDB to inspect and get data
    """

    def __init__(self):
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            sslmode=os.getenv("POSTGRES_SSLMODE", "require"),
        )
        self.cur = conn.cursor()



    def get_unique_address(self):
        """
            Get all unique address of home and work
        """
        self.cur.execute("""
            select home_address, work_address from public.user_address
        """)
        rows = self.cur.fetchall()

        merge = list({item for row in rows for item in row})
        return merge
    

    