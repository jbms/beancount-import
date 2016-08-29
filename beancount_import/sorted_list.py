from typing import TypeVar, Generic, Tuple, Iterable
import bisect
import itertools

K = TypeVar('K')
V = TypeVar('V')


class SortedList(Generic[K, V]):
    def __init__(self, items: Iterable[Tuple[K, V]]) -> None:
        entries = sorted(items, key=lambda x: x[0])
        self.keys = [x[0] for x in entries]
        self.values = [x[1] for x in entries]

    def __repr__(self) -> str:
        return repr(list(zip(self.keys, self.values)))

    def find(self, lower_bound: K, upper_bound: K) -> Iterable[V]:
        keys = self.keys
        begin_pos = bisect.bisect_left(keys, lower_bound)
        end_pos = bisect.bisect_right(keys, upper_bound)
        if begin_pos == end_pos:
            return ()
        return itertools.islice(self.values, begin_pos, end_pos)
