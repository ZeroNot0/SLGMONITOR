import argparse

from pipeline.steps.step2_mapping import run_step2


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", help="例如 0105-0111（可选）")
    parser.add_argument("--year", type=int, help="年份，例如 2025（可选）")
    args = parser.parse_args()
    run_step2(args.week, args.year)


if __name__ == "__main__":
    main()
