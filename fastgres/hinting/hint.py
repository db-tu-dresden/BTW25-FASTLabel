
from __future__ import annotations


class Hint:
    def __init__(self, name: str, index: int, database_instruction: str, instruction_value: bool):
        self.name = name
        self.index = index
        self.database_instruction = database_instruction
        self.database_instruction_value = instruction_value
        self.integer_representation = 2 ** self.index

    @property
    def tuple(self):
        return self.name, self.index, self.database_instruction, self.database_instruction_value

    @property
    def instruction_tuple(self):
        return self.database_instruction, self.database_instruction_value

    def __str__(self):
        return f"Hint({self.name}: {self.database_instruction_value})"

    def __eq__(self, other: Hint):
        return (self.name == other.name and
                self.index == other.index and
                self.database_instruction == other.database_instruction and
                self.database_instruction_value == other.database_instruction_value)

    def __lt__(self, other: Hint):
        return self.index < other.index

    def __gt__(self, other: Hint):
        return self.index > other.index
