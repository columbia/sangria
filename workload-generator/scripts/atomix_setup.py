import json
import os
import subprocess
import time
from utils import *


class AtomixSetup:
    def __init__(self):
        self.servers_config = json.load(open(SERVERS_CONFIG_PATH, "r"))
        self.servers_start_order = [
            "universe",
            "warden",
            "rangeserver",
            "resolver",
            "frontend",
        ]
        self.servers_kill_order = [
            "warden",
            "rangeserver",
            "frontend",
            "resolver",
            "universe",
        ]
        self.pids = {}

    def get_servers(self, start_order=True, servers=None):
        main_servers = (
            self.servers_start_order if start_order else self.servers_kill_order
        )
        return main_servers if servers is None else servers

    def build_servers(self):
        print("Building Atomix servers...")
        subprocess.run(["cargo", "build", "--release"], cwd=ROOT_DIR)

    def dump_servers_config(self):
        with open(RAY_SERVERS_CONFIG_PATH, "w") as f:
            json.dump(self.servers_config, f)

    def kill_servers(self, servers=None):
        if servers:
            return servers
        servers = self.get_servers(start_order=False, servers=servers)
        for server in servers:
            try:
                if server in self.pids:
                    subprocess.run(["kill", "-9", str(self.pids[server])])
                    del self.pids[server]
                    print(f"Killed '{server}'")
            except Exception as e:
                print(f"Error killing {server}: {e}")
        time.sleep(1)

    def reset_cassandra(self):
        try:
            print("Cleaning Cassandra...")
            subprocess.run(
                [
                    "sudo",
                    "docker",
                    "exec",
                    "-i",
                    "cassandra",
                    "cqlsh",
                    "-e",
                    "TRUNCATE atomix.range_map; "
                    "TRUNCATE atomix.epoch; "
                    "TRUNCATE atomix.range_leases; "
                    "TRUNCATE atomix.records; "
                    "TRUNCATE atomix.wal; "
                    "TRUNCATE atomix.transactions; "
                    "TRUNCATE atomix.keyspaces;",
                ]
            )
        except Exception as e:
            print(f"Error cleaning Cassandra: {e}")

    def start_servers(self, servers=None):
        servers = self.get_servers(start_order=True, servers=servers)
        for server in servers:
            try:
                print(f"- Spinning up {server}")
                p = subprocess.Popen(
                    [
                        RUN_CMD + server,
                        "--config",
                        str(RAY_SERVERS_CONFIG_PATH),
                    ],
                    cwd=ROOT_DIR,
                    start_new_session=True,
                    text=True,
                    env={**os.environ, "RUST_LOG": "error"},
                )
                self.pids[server] = p.pid
                time.sleep(1)
            except Exception as e:
                print(f"Error spinning up {server}: {e}")
                self.kill_servers(servers=servers)
                exit(1)
