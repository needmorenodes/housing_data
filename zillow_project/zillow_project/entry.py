import argparse
from medallion_pipeline import MedallionPipeline


def main():
    parser = argparse.ArgumentParser(
        description="Run Medallion Pipeline"
    )
    parser.add_argument(
        "--config", type=str, default="config.yaml", help="Path to configuration file"
    )
    args = parser.parse_args()

    # Run the main pipeline
    pipeline = MedallionPipeline(args.config)
    pipeline.open_connection()
    pipeline.run_pipeline()
    pipeline.conn.commit()
    pipeline.conn.close()


if __name__ == "__main__":
    main()
