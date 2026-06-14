from dataclasses import dataclass

@dataclass
class Topology:
    machines: dict[str, str]

    def get_successor(self, my_nickname: str):
        ordered = sorted(self.machines.keys())
        index = ordered.index(my_nickname)
        successor = ordered[(index + 1) % len(ordered)]
        return successor, self.machines[successor]