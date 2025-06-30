from ray.tune.search import Searcher
from typing import Dict, Optional, List, Any
import itertools
import random
from termcolor import colored
from atomix_setup import AtomixSetup
import pandas as pd

class GridSearcherInOrder(Searcher):
    def __init__(
        self,
        atomix_setup: AtomixSetup,
        num_iterations: int,
        param_grid: Dict[str, List[Any]],
        experiment_name: str,
        ray_logs_dir: str,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        super().__init__(metric=metric, mode=mode)
        self.num_iterations = num_iterations
        self.ray_logs_dir = ray_logs_dir
        self.experiment_name = experiment_name
        self.atomix_setup = atomix_setup
        self.param_keys = list(param_grid.keys())
        self.param_values = list(param_grid.values())
        self.param_keys += ["seed", "iteration"]

        # Each iteration should have a different seed
        # self.seeds = [random.randint(0, 1000000000) for _ in range(self.num_iterations)]
        start_seed = 1234567890
        self.seeds = [start_seed + i * 10 for i in range(self.num_iterations)]

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
        resolver_capacity = config["resolver_capacity"]
        config["resolver_cores"] = (
            len(resolver_capacity["background_runtime_core_ids"])
            * resolver_capacity["cpu_percentage"]
        )
        config["resolver_tx_load_concurrency"] = config["resolver_tx_load"]["max_concurrency"]
        print(colored(f"Config: {config}", "blue"))
        self.trial_map[trial_id] = config
        return config

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict], error: bool = False
    ):
        # result_df = pd.DataFrame([result])
        # path = self.ray_logs_dir / self.experiment_name / "partial_results" / f"{trial_id}.csv"
        # path.parent.mkdir(parents=True, exist_ok=True)
        # result_df.to_csv(path)
        self.trial_map.pop(trial_id, None)

    def save(self, checkpoint_path: str):
        # Optional: save state if you want to support checkpointing
        pass

    def restore(self, checkpoint_path: str):
        # Optional: restore state
        pass
