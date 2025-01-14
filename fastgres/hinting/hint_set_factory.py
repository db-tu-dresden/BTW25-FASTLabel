
from fastgres.hinting import HintLibrary, HintSet


class HintSetFactory:

    def __init__(self, hint_library: HintLibrary):

        if hint_library.collection_size <= 0:
            raise ValueError("Hint library size must be greater than zero")

        self.hint_library = hint_library

    def hint_set(self, hint_set_int: int):
        return HintSet(hint_set_int, self.hint_library)

    def default_hint_set(self):
        return self.hint_set(2**self.hint_library.collection_size-1)
