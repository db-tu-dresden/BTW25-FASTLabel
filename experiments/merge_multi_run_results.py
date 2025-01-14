import argparse
import dataclasses
import os
import pandas as pd


def check_path_exists(path: str):
    if not os.path.exists(path):
        raise ValueError(f"Path: {path} does not exist!")
    return True


class Config:
    def __init__(self, save_path: str, base_path: str, seeds: list[int], splits: list[int]):
        self.save_path = save_path
        self.base_path = base_path
        self.seeds = seeds
        self.splits = splits


@dataclasses.dataclass
class Result:
    query_name: str
    prediction: int
    label: int
    default_time: float
    opt_time: float
    context: set[frozenset]
    train_query: bool
    test_query: bool
    seed: int
    split: int


def merge(config: Config):
    dfs = list()
    for split in config.splits:
        for seed in config.seeds:
            path = config.base_path + f"_split_{split}_seed_{seed}.csv"
            check_path_exists(path)
            run_df = pd.read_csv(path)
            run_df["seed"] = seed
            run_df["split"] = split
            dfs.append(run_df)
    return pd.concat(dfs, ignore_index=True)


def run():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("base", type=str, help="")
    parser.add_argument("-o", "--output", default=None, help="")
    parser.add_argument("-s", "--seeds", type=int, nargs='+', help="")
    parser.add_argument("-ts", "--test-sizes", type=int, nargs='+', help="")
    args = parser.parse_args()

    if os.path.exists(args.output):
        raise argparse.ArgumentError(args.output, "Save path already exists.")

    config = Config(args.output, args.base, args.seeds, args.test_sizes)
    merged = merge(config)
    merged.to_csv(config.save_path, index=False)


if __name__ == "__main__":
    run()
