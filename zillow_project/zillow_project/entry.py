import argparse
from medallion_pipeline import MedallionPipeline
from dashboard import Dashboard


def main():
    parser = argparse.ArgumentParser(description="Run Medallion Pipeline")
    parser.add_argument(
        "--pipeline_config", type=str, default=None, help="Path to configuration file"
    )

    parser.add_argument(
        "--dashboard_config",
        type=str,
        default=None,
        help="Path to dashboard configuration file",
    )
    args = parser.parse_args()

    # Run the main pipeline
    if args.pipeline_config:
        pipeline = MedallionPipeline(args.pipeline_config)
        pipeline.run_pipeline()

    if args.dashboard_config:
        dashboard = Dashboard(args.dashboard_config)
        app = dashboard.build_app()
        app.run_server(debug=True, port=1222)


if __name__ == "__main__":
    main()
