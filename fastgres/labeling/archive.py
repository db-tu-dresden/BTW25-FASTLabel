
import abc
import numpy as np
import pandas as pd
import fastgres.baseline.utility as u


class Archive(abc.ABC):

    def __init__(self, archive_path: str):
        self.archive_path = archive_path

    @abc.abstractmethod
    def get_opt(self, query_name: str):
        raise NotImplementedError


class JsonArchive(Archive):

    def __init__(self, archive_path: str):
        if not archive_path.endswith(".json"):
            raise ValueError(f"Given archive path: {archive_path} is not a json file")
        super().__init__(archive_path)
        self._archive = None

    @property
    def archive(self):
        if self._archive is None:
            self._archive = u.load_json(self.archive_path)
        return self._archive

    def __getitem__(self, item):
        return self.archive[item]

    def to_dataframe(self):
        """
        Supports to convert json archive to a dataframe. Dictionary structure should look like this to support legacy
        archives:
        Query - hint set int - time
              - ...
              - opt - hint set int
        :return: dataframe with columns query_name: str, hint_set_int: int, time: float, opt: bool
        """
        entries = list()
        for query_name in self.archive.keys():
            opt_hint_set_int = int(self.archive[query_name]["opt"])
            for hint_set_int in self.archive[query_name].keys():
                if "opt" in hint_set_int:
                    continue
                entry = dict()
                entry["query_name"] = query_name
                entry["hint_set_int"] = hint_set_int
                entry["time"] = self.archive[query_name][str(hint_set_int)]
                if int(hint_set_int) == opt_hint_set_int:
                    entry["opt"] = True
                else:
                    entry["opt"] = False
                entries.append(entry)
        return pd.DataFrame.from_dict(entries)

    def get_opt(self, query_name: str):
        return self.archive[query_name]["opt"]


class DataframeArchive(Archive):

    def __init__(self, archive_path: str):
        super().__init__(archive_path)
        self._archive = None
        self._hints_used = None
        self._archive_dict = {key: dict() for key in np.unique(self.archive["query_name"])}

    @property
    def hints_used(self):
        if self._hints_used is None:
            max_hint_set_int = max(self.archive["hint_set_int"].tolist())
            self._hints_used = int(np.log2(max_hint_set_int+1))
        return self._hints_used

    @property
    def archive(self):
        if self._archive is None:
            self._archive = pd.read_csv(self.archive_path)
        return self._archive

    def to_json(self):
        json_cols = ["query_name", "hint_set_int", "time", "opt"]
        reduced_df = self.archive[json_cols]
        query_names = np.unique(reduced_df["query_name"].to_list())

        archive_dict = {query_name: dict() for query_name in query_names}
        opts = dict()
        for entry in reduced_df.to_numpy():
            query_name, hint_set, time, opt = entry
            if opt:
                opts[query_name] = hint_set
            archive_dict[query_name][str(hint_set)] = time
        for query_name, hint_set in opts.items():
            archive_dict[query_name]["opt"] = hint_set

        return archive_dict

    def get_opt(self, query_name: str):
        try:
            return self._archive_dict[query_name]["opt"]
        except KeyError:
            for key, value in self.archive[self.archive["opt"] == True][["query_name", "hint_set_int"]].to_numpy():
                self._archive_dict[key]["opt"] = value
        return self._archive_dict[query_name]["opt"]

    def get_opt_time(self, query_name: str):
        try:
            return self._archive_dict[query_name]["opt_time"]
        except KeyError:
            for key, value in self.archive[self.archive["opt"] == True][["query_name", "time"]].to_numpy():
                self._archive_dict[key]["opt_time"] = value
        return self._archive_dict[query_name]["opt_time"]

    def get_default_time(self, query_name: str):
        try:
            return self._archive_dict[query_name]["def_time"]
        except KeyError:
            for key, value in self.archive[self.archive["hint_set_int"] == 2**self.hints_used-1][["query_name", "time"]].to_numpy():
                self._archive_dict[key]["def_time"] = value
        return self._archive_dict[query_name]["def_time"]

    def get_query_entries(self, query_name: str):
        return self.archive[(self.archive["query_name"] == query_name) & (self.archive["timeout"] == False)]
