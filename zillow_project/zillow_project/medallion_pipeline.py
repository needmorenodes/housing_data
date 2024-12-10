import duckdb
import yaml
from pathlib import Path
import pandas as pd
from datetime import datetime
import glob
import pandas as pd
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path


class MedallionPipeline:
    def __init__(self, config_path: str):
        try:
            with open(config_path, "r") as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Configuration file not found at {config_path}"
            )
        self._validate_config()
        self._initialize_database()

    def _initialize_database(self):
        if self.config["database"]["type"].lower() != "duckdb":
            raise ValueError("Only DuckDB is supported at this time")

        db_path = self.config["database"]["path"]
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            # Connect to database (creates it if it doesn't exist)
            conn = duckdb.connect(db_path)
            conn.close()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize database: {str(e)}")

    def _validate_config(self):
        required_configs = {
            "database": {"path", "type"},
        }

        if "database" not in self.config:
            raise ValueError("Database configuration is missing")

        for main_config, sub_configs in required_configs.items():
            if main_config not in self.config:
                raise ValueError(
                    f"Required key '{main_config}' is missing in config file"
                )

            if main_config == "database":
                missing_keys = sub_configs - set(self.config[main_config].keys())
                if missing_keys:
                    raise ValueError(f"Missing required database configuration: {missing_keys}")

    def _check_if_table_exists(self, tablename):
        cur = self.conn.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{tablename}'
            """)
        result = cur.fetchone()[0]
        if result == 1:
            return True
        return False

    def open_connection(self):
        # Could consider a generator function here to not leave hanging connections open
        self.conn = duckdb.connect(self.config["database"]["path"])

    # Dataset specific readers

    # Reads from disk in the expected format <type>_<end_month><year>.xlsx
    def _load_hud_crosswalk_from_local(self, local_path, year, required_columns):
        if year == "ALL":
            files_to_load = glob.glob(f"{local_path}/*.xlsx")
        else:
            # Find all crosswalk files in the directory
            files_to_load = glob.glob(f"{local_path}/*{year}.xlsx")

        # Build dataframes with start and end dates
        dfs = []

        # Groups 1 and 2 were for columns in the dataset. Moved to config file.
        pattern = r"(\w+)_(\w+)_(\d{2})(\d{4})\.xlsx"
        for file in files_to_load:
            print(f"\t\t\tProcessing File: {file}")
            # Pull the month out of the expected file format
            match = re.search(pattern, file)
            if not match:
                raise ValueError("No month found in the file path")
            month = match.group(3)
            file_year = match.group(4)
            date = datetime.strptime(f"{file_year}-{month}-01", "%Y-%m-%d")
            start_date = date - relativedelta(months=2)

            if month == "12":
                end_date = date.replace(month=12, day=31)
            else:
                end_date = date.replace(month=date.month + 1, day=1) - timedelta(days=1)

            load_quarter = f"{month}{file_year}"

            # Load the XLSX, apply the start and end dates. Union, and Return
            df = pd.read_excel(file)[required_columns]
            df["start_date"] = start_date.strftime("%Y-%m-%d")
            df["end_date"] = end_date.strftime("%Y-%m-%d")
            df["load_quarter"] = load_quarter
            dfs += [df]
        df_ret = pd.concat(dfs, axis=0, ignore_index=True)
        return df_ret

    # Reads from disk in the expected format SVI_<year>_US.csv
    def _load_svi_from_local(local_path, year):
        # Find all crosswalk files in the directory
        files_to_load = glob.glob(f"{local_path}/SVI_{year}_US.csv")

        if not files_to_load:
            raise ValueError(f"No file found for year {year} in {local_path}!")
        if len(files_to_load) > 1:
            raise ValueError(
                f"Multiple files found for {year} in {local_path}!\n{files_to_load}"
            )

        file = files_to_load[0]
        df_ret = pd.read_csv(file)[
            [
                "STATE",
                "ST_ABBR",
                "STCNTY",
                "COUNTY",
                "FIPS",
                "LOCATION",
                "RPL_THEME1",
                "RPL_THEME2",
                "RPL_THEME3",
                "RPL_THEME4",
                "RPL_THEMES",
            ]
        ]
        df_ret["year"] = year
        return df_ret

    def _load_zillow_from_local(local_path, zillow_file):
        # Find all crosswalk files in the directory
        files_to_load = glob.glob(f"{local_path}/{zillow_file}")

        if not files_to_load:
            raise ValueError(f"No file found for year {zillow_file} in {local_path}!")
        if len(files_to_load) > 1:
            # Pick the most recent one here probably?
            raise ValueError(
                f"Multiple files found for {zillow_file} in {local_path}!\n{files_to_load}"
            )

        file = files_to_load[0]
        df_ret = pd.read_csv(file)
        return df_ret

    # Bronze Load

    # Load the crosswalk tables into bronze
    def _load_crosswalk_bronze(self):
        # Need to add validation for all bronze....
        crosswalk_tables = self.config['sources']['crosswalk']
        for crosswalk_table, config in crosswalk_tables.items():
            print(f"\t\tLoading {crosswalk_table}")
            source_path = config['path']
            year = config['year']
            required_columns = config['required_columns']

            loaded_crosswalk_df = self._load_hud_crosswalk_from_local(source_path, year, required_columns)
            if self._check_if_table_exists(crosswalk_table):
                print(f"\t\t\tTable Found, Removing overlapping quarters to process {crosswalk_table}")
                self.conn.sql(
                    f"DELETE FROM {crosswalk_table} WHERE load_quarter IN (SELECT DISTINCT load_quarter FROM loaded_crosswalk_df)"
                )
            else:
                print(f"\t\t\tTable Not Found, Creating {crosswalk_table} with schema {config['table_schema']}")
                self.conn.sql(f"CREATE OR REPLACE TABLE {crosswalk_table} ({config['table_schema']})")
            
            print(f"\t\t\tLoading data into {crosswalk_table}")
            self.conn.sql(f"""
                INSERT INTO {crosswalk_table} FROM
                    (SELECT * FROM loaded_crosswalk_df);
            """)

    def _load_svi_bronze(self, year):
        df_svi = self._load_svi_from_local("../chosen_data/svi/", year)

        self.conn.sql(
            "DELETE FROM svi WHERE year IN (SELECT DISTINCT year FROM df_svi)"
        )

        self.conn.sql(
            """
        INSERT INTO svi FROM
            (SELECT * FROM df_svi);
        """
        )

    def _load_zillow_bronze(self, file_path, file_name):
        df_zillow_zhvi_sfr_zip = self._load_zillow_from_local(file_path, i)
        self.conn.sql(
            """
            CREATE OR REPLACE TABLE zillow_zhvi_sfr_zip_bronze AS
                SELECT * FROM df_zillow_zhvi_sfr_zip;
        """
        )

    def load_bronze(self):
        # Parse Config for Bronze Load
        print("\tLoading Bronze Crosswalk")
        self._load_crosswalk_bronze()
        # self._load_svi_bronze()
        # self._load_zillow_bronze()

    def run_pipeline(self):
        # Determine what we need to load based on the configuration?
        # Allow for the independent loading for a scheduling framework
        print("Loading Bronze")
        self.load_bronze()
