
import numpy as np
from fastgres.hinting import HintLibrary, Hint


class HintSet:
    def __init__(self, hint_set_int: int, hint_library: HintLibrary):

        if isinstance(hint_set_int, np.integer):
            hint_set_int = int(hint_set_int)

        if not isinstance(hint_set_int, int):
            raise ValueError(f'Input {hint_set_int} is of type {type(hint_set_int)} not int')

        self.collection = hint_library
        self.hints_used = self.collection.collection_size
        self.instructions = self.collection.get_instructions()

        if not 0 <= hint_set_int < 2**self.hints_used:
            raise ValueError(f"Hint Set Integer: {hint_set_int} out of bounds for {self.hints_used} hints")

        [self.__setattr__(hint.name, hint.database_instruction_value) for hint in self.collection.get_hints()]

        self.hint_set_int = hint_set_int
        self.hint_set_from_int()

    def __str__(self):
        return f"Hint Set: {self.hint_set_int} : {self.get_boolean_representation()}"

    def print_info(self):
        print(f"Hint Set: {self.hint_set_int}")
        print("Hint Attributes:")
        [print(f"{hint.name} "
               f"(default: {hint.database_instruction_value}): "
               f"{self.get(hint.index)}") for hint in self.collection.get_hints()]
        return

    def hint_set_from_int(self):
        binary = self.get_binary()
        self.hint_set_from_int_list(binary)

    def hint_set_from_int_list(self, binary: list[int]):
        uniques = np.unique(binary)
        if not (set(uniques).issubset({0, 1})):
            raise ValueError(f"Trying to set hint from non binary int list {binary}")
        [self._flip_hint(index) for index in range(self.hints_used) if binary[index] == 0]

    def _flip_hint(self, index: int):
        self._set_hint_i(index, not self.get(index))

    def get_boolean_representation(self) -> list[int]:
        return [self.get(i) for i in range(self.hints_used)]

    def get_binary(self) -> list[int]:
        value = self.hint_set_int
        return list(reversed([int(i) for i in bin(value)[2:].zfill(self.hints_used)]))

    def get_hint(self, index: int) -> Hint:
        try:
            return self.collection.hints[index]
        except KeyError:
            raise KeyError(f"Trying to access hint index: {index} "
                           f"that is not in collection with indices: {self.collection.hints.keys()}")

    def get(self, index: int) -> bool:
        try:
            return self.__getattribute__(self.collection.hints[index].name)
        except KeyError:
            raise KeyError(f"Trying to access attribute index: {index} "
                           f"that is not in collection with indices: {self.collection.hints.keys()}")

    def _set_hint_i(self, index: int, value: bool):
        if value not in [True, False]:
            raise ValueError('Trying to set hint set from non boolean')
        if index not in range(len(self.instructions)):
            raise ValueError(f'Index {index} is out of bounds for {len(self.instructions)} operators')
        if index not in self.collection.hints.keys():
            raise ValueError(f'Index {index} is not a valid hint index in collection '
                             f'with indices: {self.collection.hints.keys()}')
        self.__setattr__(self.get_hint(index).name, value)
        return
