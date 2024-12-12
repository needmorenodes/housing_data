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
from contextlib import contextmanager
from dataclasses import dataclass
from typing import List


@dataclass
class Source:
    """Dataclas to hold the configuration for the source (bronze) tables.

    Attributes:
        table_name (str): Name of the destination table.
        path (str): Path to the data file/files.
        year (int): Year being processed.
        required_columns (str): Required source columns to keep.
        table_schema (str): Schema of the desired output table.
    """

    table_name: str
    type: str
    path: str
    year: int
    required_columns: List[str]
    table_schema: str


@dataclass
class Transformation:
    """Dataclass to hold the configuration for the transformation (silver) tables.

    Attributes:
        table_name (str): Name of the destination table.
        transform (str): Type of transformation CREATE_OR_REPLACE or INSERT.
        sql (str): SQL statement to be injected into the CREATE OR RELACE or INSERT statement.
    """

    table_name: str
    transform: str
    sql: str


class MedallionPipeline:
    """Contains the code to connect to the configured database and run the configured pipeline.

    Attributes:
        config_path (str): Path to the YAML configuration for this class.
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        try:
            with open(config_path, "r") as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at {config_path}")
        self._validate_config()
        self._initialize_database()

    def _validate_config(self):
        """Validates the config and populates the sources and transformations."""
        print(f"Validating configs from {self.config_path}")
        required_configs = {
            "database": {"path", "type"},
        }

        # The only true required config is the database configuration
        # TODO: Confirmation could be more genaric based on the dataclasses above
        if "database" not in self.config:
            raise ValueError("Database configuration is missing")

        for main_config, sub_configs in required_configs.items():
            if main_config not in self.config:
                raise ValueError(
                    f"Required key '{main_config}' is missing in config file"
                )

            if main_config == "database":
                missing_keys = sub_configs - self.config[main_config].keys()
                if missing_keys:
                    raise ValueError(
                        f"Missing required database configuration: {missing_keys}"
                    )

        # Build the source configs and set them on the class
        sources = []
        if "sources" in self.config:
            required_source_congifs = [
                "type",
                "path",
                "year",
                "required_columns",
                "table_schema",
            ]
            for table_name, config in self.config["sources"].items():
                missing_keys = required_source_congifs - config.keys()
                if missing_keys:
                    raise ValueError(
                        f"Missing required source configuration: {missing_keys} for source: {table_name}"
                    )
                sources += [
                    Source(
                        table_name=table_name,
                        type=config["type"],
                        year=config["year"],
                        path=config["path"],
                        required_columns=config["required_columns"],
                        table_schema=config["table_schema"],
                    )
                ]
            self.sources = sources
        print(
            f"\tParsed the following source (Bronze) tables: {', '.join([source.table_name for source in sources])}"
        )

        # Build the transformation configs and set them on the class
        transformations = []
        if "transformations" in self.config:
            required_transformation_congifs = ["transform", "sql"]
            for table_name, config in self.config["transformations"].items():
                missing_keys = required_transformation_congifs - config.keys()
                if missing_keys:
                    raise ValueError(
                        f"Missing required source configuration: {missing_keys} for source: {table_name}"
                    )
                transformations += [
                    Transformation(
                        table_name=table_name,
                        transform=config["transform"],
                        sql=config["sql"],
                    )
                ]
            self.transformations = transformations
        print(
            f"\tParsed the following transformation (Silver) tables: {', '.join([transformation.table_name for transformation in transformations])}"
        )

    def _initialize_database(self):
        """Initializes the database connection.

        Will confirm the database file exists and is accessable or will initialize the database.
        """

        # Assuming the database has been validated by _validate_config...
        # TODO: Would be good to store in a dataclass that could be extended for future Database connections and then validated by _validate_config.
        if self.config["database"]["type"].lower() != "duckdb":
            raise ValueError("Only DuckDB is supported at this time")

        # Attempt to mkdirs the path if not exist
        db_path = self.config["database"]["path"]
        self.database = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            print(f"Initializing database at {db_path}")
            # Connect to database (creates it if it doesn't exist)
            conn = duckdb.connect(db_path)
            conn.close()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize database: {str(e)}")

    def _check_if_table_exists(self, conn, tablename):
        """Checks if a table exists in the database

        Attributes:
            conn: Database connection
            table_name: Name of the table to check if exists.
        """
        cur = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{tablename}'
            """
        )
        result = cur.fetchone()[0]
        if result == 1:
            return True
        return False

    @contextmanager
    def _open_connection(self):
        """Opens a connection to the database and yields the connection to be commited and closed when complete."""
        conn = duckdb.connect(self.config["database"]["path"])
        conn.sql("SET enable_progress_bar = true")
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()

    # Dataset specific readers

    # Reads from disk in the expected format <type>_<end_month><year>.xlsx
    def _load_hud_crosswalk_from_local(self, source):
        """Function for loading the crosswalk XLSX files and returning a pandas dataframe."""
        if source.year == "ALL":
            files_to_load = glob.glob(f"{source.path}/*.xlsx")
        else:
            # Find all crosswalk files in the directory
            files_to_load = glob.glob(f"{source.path}/*{source.year}.xlsx")

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
            df = pd.read_excel(file)
            df.columns = map(str.upper, df.columns)
            df = df[source.required_columns]

            df["start_date"] = start_date.strftime("%Y-%m-%d")
            df["end_date"] = end_date.strftime("%Y-%m-%d")
            df["load_quarter"] = load_quarter
            dfs += [df]
        df_ret = pd.concat(dfs, axis=0, ignore_index=True)
        return df_ret

    # Reads from disk in the expected format SVI_<year>_US.csv
    def _load_svi_from_local(self, source):
        """Function for loading the svi csv files and returning a pandas dataframe."""

        if source.year == "ALL":
            # Find all SVI files in the directory
            files_to_load = glob.glob(f"{source.path}/*.csv")
        else:
            files_to_load = glob.glob(f"{source.path}/SVI_{source.year}_US.csv")

        # Find if there are multiple files for the same year
        for file in files_to_load:
            if not files_to_load:
                raise ValueError(
                    f"No file found for year {source.year} in {source.path}!"
                )
            if len(files_to_load) > 1:
                raise ValueError(
                    f"Multiple files found for {source.year} in {source.path}!\n{files_to_load}"
                )

        file = files_to_load[0]
        df_ret = pd.read_csv(file)[source.required_columns]
        df_ret["year"] = source.year
        return df_ret

    def _load_zillow_from_local(self, source):
        """Function for loading the zillow csv files and returning a pandas dataframe."""

        # Find all crosswalk files in the directory
        files_to_load = glob.glob(f"{source.path}")

        if not files_to_load:
            raise ValueError(f"No file found for year in {source.path}!")

        file = files_to_load[0]
        df_ret = pd.read_csv(file)
        required_columns = source.required_columns + [
            column for column in df_ret.columns if column.startswith(str(source.year))
        ]
        return df_ret[required_columns]

    # Bronze Load

    # Load the crosswalk tables into bronze
    def _load_crosswalk_bronze(self, sources):
        """Function for processing crosswalk files to bronze."""

        with self._open_connection() as conn:
            # Need to add validation for all bronze....
            for source in sources:
                print(f"\t\tLoading {source.table_name}")
                loaded_crosswalk_df = self._load_hud_crosswalk_from_local(source)

                if self._check_if_table_exists(conn, source.table_name):
                    print(
                        f"\t\t\tTable Found, Removing overlapping quarters to process {source.table_name}"
                    )
                    conn.sql(
                        f"DELETE FROM {source.table_name} WHERE load_quarter IN (SELECT DISTINCT load_quarter FROM loaded_crosswalk_df)"
                    )
                else:
                    print(
                        f"\t\t\tTable Not Found, Creating {source.table_name} with schema {source.table_schema}"
                    )
                    conn.sql(
                        f"CREATE OR REPLACE TABLE {source.table_name} ({source.table_schema})"
                    )

                print(f"\t\t\tLoading data into {source.table_name}")
                conn.sql(
                    f"""
                    INSERT INTO {source.table_name} FROM
                        (SELECT * FROM loaded_crosswalk_df);
                """
                )

    def _load_svi_bronze(self, sources):
        """Function for processing svi files to bronze."""
        for source in sources:
            print(f"\t\tLoading svi table {source.table_name}")
            df_svi = self._load_svi_from_local(source)

            with self._open_connection() as conn:
                if self._check_if_table_exists(conn, source.table_name):
                    print(
                        f"\t\t\tTable Found, Removing overlapping quarters to process {source.table_name}"
                    )
                    conn.sql(
                        f"DELETE FROM {source.table_name} WHERE year = {source.year}"
                    )
                else:
                    print(
                        f"\t\t\tTable Not Found, Creating {source.table_name} with schema {source.table_schema}"
                    )
                    conn.sql(
                        f"CREATE OR REPLACE TABLE {source.table_name} ({source.table_schema})"
                    )
                conn.sql(
                    "DELETE FROM svi WHERE year IN (SELECT DISTINCT year FROM df_svi)"
                )

                print(f"\t\t\tLoading data into {source.table_name}")
                conn.sql(
                    """
                INSERT INTO svi FROM
                    (SELECT * FROM df_svi);
                """
                )

    # Ingesting and pivoting bronze immediatly to allow for easier ingestion of the data
    def _load_zillow_bronze(self, sources):
        """Function for processing zillow files to bronze."""

        for source in sources:
            with self._open_connection() as conn:
                print(f"\t\tLoading zillow tables")
                zillow_df = self._load_zillow_from_local(source)

                if self._check_if_table_exists(conn, source.table_name):
                    print(
                        f"\t\t\tTable Found, Removing overlapping years to process {source.table_name}"
                    )
                    conn.sql(
                        f"DELETE FROM {source.table_name} WHERE year(date::DATE) = {source.year}"
                    )
                else:
                    print(
                        f"\t\t\tTable Not Found, Creating {source.table_name} with schema {source.table_schema}"
                    )
                    conn.sql(
                        f"CREATE OR REPLACE TABLE {source.table_name} ({source.table_schema})"
                    )

                print(f"\t\t\tLoading data into {source.table_name}")
                conn.sql(
                    f"""
                    INSERT INTO {source.table_name} FROM
                        (UNPIVOT zillow_df
                            ON COLUMNS(* EXCLUDE ({','.join(source.required_columns)}))
                            INTO
                            NAME date
                            VALUE zhvi
                        );
                    """
                )

    def load_bronze(self):
        """Function for loading all bronze/source data"""
        crosswalk_sources = [
            source for source in self.sources if source.type == "crosswalk"
        ]
        if crosswalk_sources:
            print(
                f"\tLoading Bronze Crosswalk Tables [{', '.join([source.table_name for source in crosswalk_sources])}]"
            )
            self._load_crosswalk_bronze(crosswalk_sources)

        svi_sources = [source for source in self.sources if source.type == "svi"]
        if svi_sources:
            print(
                f"\tLoading Bronze SVI Tables [{', '.join([source.table_name for source in svi_sources])}]"
            )
            self._load_svi_bronze(svi_sources)

        zillow_sources = [source for source in self.sources if source.type == "zillow"]
        if svi_sources:
            print(
                f"\tLoading Bronze Zillow Tables [{', '.join([source.table_name for source in zillow_sources])}]"
            )
            self._load_zillow_bronze(zillow_sources)

    def load_transformations(self):
        """Function for loading/creating all silver/transformation tables"""

        for transformation in self.transformations:
            with self._open_connection() as conn:
                print(f"\tLoading Silver Transformation: {transformation.table_name}")
                if transformation.transform == "CREATE_OR_REPLACE":
                    conn.sql(
                        f"CREATE OR REPLACE TABLE {transformation.table_name} AS ({transformation.sql})"
                    )
                elif transformation.transform == "INSERT":
                    conn.sql(
                        f"INSERT INTO {transformation.table_name} FROM ({transformation.sql})"
                    )
                else:
                    raise ValueError(
                        "Unsupported Transformation, only INSERT and CREATE_OR_REPLACE supported"
                    )

    def run_pipeline(self):
        # Determine what we need to load based on the configuration?
        # Allow for the independent loading for a scheduling framework
        print("\nLoading Bronze")
        self.load_bronze()

        print("Loading Transfomations")
        self.load_transformations()
        print(f"Pipeline complete, database at {self.database}!")
