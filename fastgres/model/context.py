from __future__ import annotations


class Context:

    def __init__(self, table_set: frozenset = None, name: str = "default_context_name"):
        self.covered_contexts = set()
        self.is_merged = False
        self.table_sets = 0
        self.total_tables = frozenset()
        self.name = name
        if table_set:
            self.add_context(table_set)

    def to_dict(self):
        return {
            "covered_contexts": list(self.covered_contexts),
            "is_merged": self.is_merged,
            "table_sets": self.table_sets,
            "total_tables": self.total_tables,
            "name": self.name,
        }

    @classmethod
    def from_dict(cls, context_dict):
        context = cls()
        context.covered_contexts = set(context_dict["covered_contexts"])
        context.is_merged = context_dict["is_merged"]
        context.table_sets = context_dict["table_sets"]
        context.total_tables = context_dict["total_tables"]
        context.name = context_dict["name"]
        return context

    def __hash__(self):
        return sum([hash(c) for c in self.covered_contexts])

    def __str__(self):
        return_string = f"Context {self.name} covering {self.table_sets} table sets "
        for _ in self.covered_contexts:
            return_string += str(_) + " "
        return_string += f"Encompassing {len(self.total_tables)} tables"
        return return_string

    def __eq__(self, other):
        if not isinstance(other, Context):
            raise ValueError("Compared value is not of class Context.")
        # Third comparison might be redundant
        return all([_ in self.covered_contexts for _ in other.covered_contexts]) \
            and self.table_sets == other.table_sets \
            and self.total_tables == other.total_tables

    def add_context(self, table_set: frozenset) -> None:
        self.covered_contexts.add(table_set)
        if table_set.issubset(self.total_tables) or not self.total_tables:
            self.table_sets += 1
        self.total_tables = self.total_tables.union(table_set)
        if self.table_sets != 1:
            self.is_merged = True

    def merge(self, context: Context) -> None:
        for table_set in context.covered_contexts:
            self.add_context(table_set)

    def get_context_histogram(self) -> dict:
        context_dictionary = dict()
        for sub_context in self.covered_contexts:
            context_size = len(sub_context)
            if context_size in context_dictionary:
                context_dictionary[context_size] += 1
            else:
                context_dictionary[context_size] = 1
        return context_dictionary
