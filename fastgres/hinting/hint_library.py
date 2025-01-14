
from __future__ import annotations
from typing import Optional
from fastgres.hinting import Hint


class HintLibrary:
    def __init__(self, hint_list: Optional[list[Hint]]):
        self.hints = dict()
        self._collection_size = 0
        if hint_list is not None:
            [self.add_hint(hint) for hint in hint_list]
            if not self.verify_integrity():
                self.verify_integrity(verbose=True)
                raise ValueError(f"Hint Collection input: {[_.name for _ in hint_list]} "
                                 f"does not fulfill integrity constraints")

    @property
    def collection_size(self) -> int:
        return self._collection_size

    def add_hints(self, hints: list[Hint]):
        for hint in hints:
            self.add_hint(hint)

    def add_hint(self, hint: Hint) -> None:
        if hint.index in self.hints:
            raise ValueError(f"Hint with value: {hint.index} is already present in HintCollection.")
        self.hints[hint.index] = hint
        self._collection_size += 1

    def remove_hint(self, hint: Hint) -> None:
        if hint.index not in self.hints:
            raise ValueError(f"Hint with value: {hint.index} is not present in HintCollection.")
        entry = self.hints[hint.index]
        if entry != hint:
            raise ValueError(f"Entry {entry} and hint to remove {hint} missmatch")
        self.remove_from_index(hint.index)

    def remove_from_index(self, index: int) -> None:
        if index not in self.hints:
            raise ValueError(f"Index with value: {index} is not present in HintCollection.")
        del self.hints[index]
        self._collection_size -= 1

    def get_hints(self) -> list[Hint]:
        return [self.hints[idx] for idx in sorted(self.hints.keys(), reverse=False)]

    def get_hint_names(self) -> list[str]:
        return [self.hints[idx].name for idx in sorted(self.hints.keys(), reverse=False)]

    def get_values(self) -> list[int]:
        return [self.hints[idx].integer_representation for idx in sorted(self.hints.keys(), reverse=False)]

    def get_instructions(self) -> list[str]:
        return [self.hints[idx].database_instruction for idx in sorted(self.hints.keys(), reverse=False)]

    def get_instruction_values(self) -> list[int]:
        return [self.hints[idx].database_instruction_value for idx in sorted(self.hints.keys(), reverse=False)]

    def get_tuples(self) -> list[tuple]:
        return [self.hints[idx].tuple for idx in sorted(self.hints.keys(), reverse=False)]

    def verify_integrity(self, verbose=False) -> bool:
        """
        Verifies different aspects needed in hint collections like continuous hint value order,
        overlapping instructions, or invalid default initializations.
        :param verbose: Whether to print single integrity information or not
        :return: True if all integrity constraints have passed else False.
        """

        index_set = set(self.hints.keys())
        max_index = max(index_set)
        sound_indices = {i for i in range(max_index + 1)}
        instructions = self.get_instructions()
        default_instruction_values = self.get_instruction_values()

        ascending_hint_integrity = False if index_set.difference(sound_indices) else True
        ascending_hint_integrity = ascending_hint_integrity and (self.collection_size == max_index + 1)
        overlapping_instructions = True if len(instructions) == self.collection_size else False

        _default_instruction_integrity = [True if entry in [True, False] else False
                                          for entry in default_instruction_values]
        default_value_integrity = False if False in _default_instruction_integrity else True

        if verbose:
            print(f"Ascending Index Integrity: {ascending_hint_integrity}")
            print(f"No Overlapping Instruction Integrity: {overlapping_instructions}")
            print(f"Initial Hint Value Integrity: {default_value_integrity}")

        return ascending_hint_integrity and overlapping_instructions and default_value_integrity
