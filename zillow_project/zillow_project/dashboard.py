from dash import Dash, dcc, html, Input, Output, callback
import plotly.express as px
import duckdb
import yaml
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from contextlib import contextmanager


class Dashboard:
    """Contains the code to build and start the dashboard.

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

    # Repeat code from the medallion pipeline, could be a utility
    def _validate_config(self):
        """Validates the config and populates the sources and transformations."""
        print(f"Validating configs from {self.config_path}")
        required_configs = {
            "database": {"path", "type"},
        }

        # The only true required config is the database configuration
        # TODO: Configuration could be more genaric based on the dataclasses above
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

    def build_app(self):
        """Starts a Dash App to show the dashboard"""
        # Create sample app
        app = Dash(__name__)

        # Collect distinct years and states for dropdown
        years = []
        states = []
        with self._open_connection() as conn:
            years = (
                conn.sql(
                    "SELECT DISTINCT year(date::DATE) as year FROM zhvi_sfr_zip_with_svi ORDER BY year(date::DATE)"
                )
                .df()
                .year.unique()
            )
            states = (
                conn.sql(
                    "SELECT DISTINCT StateName FROM zhvi_sfr_zip_with_svi ORDER BY StateName"
                )
                .df()
                .StateName.unique()
            )

        # Known available svi themes
        # TODO: Could be worked into configuration
        themes = [
            "avg_rpl_theme1",
            "avg_rpl_theme2",
            "avg_rpl_theme3",
            "avg_rpl_theme4",
            "avg_rpl_themes",
        ]

        # Create layout with dropdown and graph
        # 3 Dropdowns for Year, State, and Theme
        app.layout = html.Div(
            [
                html.Div(
                    [
                        dcc.Dropdown(years, id="pandas-dropdown-1", value=2020),
                        html.Div(id="pandas-output-container-1"),
                    ]
                ),
                html.Div(
                    [
                        dcc.Dropdown(states, id="pandas-dropdown-2"),
                        html.Div(id="pandas-output-container-2"),
                    ]
                ),
                html.Div(
                    [
                        dcc.Dropdown(
                            themes, id="pandas-dropdown-3", value="avg_rpl_themes"
                        ),
                        html.Div(id="pandas-output-container-3"),
                    ]
                ),
            ]
        )

        @callback(
            Output("pandas-output-container-3", "children"),
            Input("pandas-dropdown-1", "value"),
            Input("pandas-dropdown-2", "value"),
            Input("pandas-dropdown-3", "value"),
        )
        def update_output(year, state, rpl_theme):
            """Update function to query the database and generate the visualizations to display"""
            with self._open_connection() as conn:
                # Retrieve the data from the database and store as a local DF
                # TODO: Pushing the filtering of year and state will likely be more performant on other systems
                #       For this case we store the entire updated dataframe in the app.
                render_df = conn.sql(
                    f"""
                    SELECT * FROM (
                    SELECT 
                        StateName,
                        zip::string as zip,
                        year(date::DATE) as year, 
                        avg(zhvi) as avg_zhvi, 
                        avg(avg_rpl_theme1) as avg_rpl_theme1, 
                        avg(avg_rpl_theme2) as avg_rpl_theme2, 
                        avg(avg_rpl_theme3) as avg_rpl_theme3, 
                        avg(avg_rpl_theme4) as avg_rpl_theme4, 
                        avg(avg_rpl_themes) as avg_rpl_themes 
                    FROM zhvi_sfr_zip_with_svi 
                    GROUP BY StateName, zip, year(date::DATE)
                    ORDER BY avg_zhvi)
                    WHERE avg_zhvi is not null AND {rpl_theme} IS NOT NULL"""
                ).df()

            # Validate the dataframe did not return empty
            if not render_df.empty:
                render_df = render_df.loc[render_df["year"] == year]
                render_df = render_df.loc[render_df["StateName"] == state]
                render_df["idx"] = range(1, len(render_df) + 1)

                # Create figure with 2 subplots
                fig = make_subplots(rows=2, cols=1)
                fig.update_layout(height=1000, bargap=0)

                # Add first bar chart showing Average ZHVI ordered by least to greatest
                fig.add_trace(
                    go.Bar(
                        x=render_df.zip,
                        y=render_df.avg_zhvi,
                        name="Average ZHVI",
                    ),
                    row=1,
                    col=1,
                )

                # Add second bar chart showing the average chosen SVI theme ordered from greatest to least
                fig.add_trace(
                    go.Bar(
                        x=render_df.zip,
                        y=render_df[rpl_theme],
                        name="Average SVI Theme",
                    ),
                    row=2,
                    col=1,
                )

                # Build a trend line ontop of the SVI theme and then render ontop of the SVI barchart to show trend
                fig_scatter = px.scatter(
                    render_df,
                    x="idx",
                    y=rpl_theme,
                    trendline="ols",
                    trendline_color_override="black",
                )
                trendline = fig_scatter.data[1]
                trendline.x = list(render_df.zip)
                fig.add_trace(trendline, row=2, col=1)
            else:
                raise RuntimeError("Database cannot be queried.")
            return dcc.Graph(figure=fig)

        return app
