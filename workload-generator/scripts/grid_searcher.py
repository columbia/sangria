from ray.tune.search import Searcher
from typing import Dict, Optional, List, Any
import itertools
import random
from termcolor import colored
from atomix_setup import AtomixSetup


class GridSearcherInOrder(Searcher):
    def __init__(
        self,
        atomix_setup: AtomixSetup,
        num_iterations: int,
        param_grid: Dict[str, List[Any]],
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        super().__init__(metric=metric, mode=mode)
        self.num_iterations = num_iterations
        self.atomix_setup = atomix_setup
        self.param_keys = list(param_grid.keys())
        self.param_values = list(param_grid.values())
        self.param_keys.append("seed")
        self.param_keys.append("iteration")

        # Each iteration should have a different seed
        self.seeds = [random.randint(0, 1000000000) for _ in range(self.num_iterations)]

        self.grid = list(itertools.product(*self.param_values))
        self.grid = [
            list(config) + [self.seeds[iteration], iteration]
            for config in self.grid
            for iteration in range(self.num_iterations)
        ]

        self.trial_queue = self.grid
        self.trial_map = {}

    def suggest(self, trial_id: str) -> Optional[Dict[str, Any]]:
        if not self.trial_queue:
            return None
        values = self.trial_queue.pop(0)
        config = dict(zip(self.param_keys, values))
        print(colored(f"Config: {config}", "blue"))
        self.trial_map[trial_id] = config
        return config

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict], error: bool = False
    ):
        self.trial_map.pop(trial_id, None)

    def save(self, checkpoint_path: str):
        # Optional: save state if you want to support checkpointing
        pass

    def restore(self, checkpoint_path: str):
        # Optional: restore state
        pass
