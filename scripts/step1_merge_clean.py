import argparse

from pipeline.steps.step1_merge_clean import run_step1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", required=True, help="例如 0105-0111")
    parser.add_argument("--year", type=int, help="年份，例如 2025（可选，会自动检测）")
    args = parser.parse_args()
    run_step1(args.week, args.year)


if __name__ == "__main__":
    main()
