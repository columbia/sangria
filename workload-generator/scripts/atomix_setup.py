import json
import os
import subprocess
import time
import socket
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
            "rangeserver",
            "warden",
            "frontend",
            "resolver",
            "universe",
        ]
        self.pids = {}

    def wait_for_port_release(self, server, timeout=5):

        print(f"Waiting for {server} to release port...")
        match server:
            case "warden":
                server_address = self.servers_config["regions"]["test-region"][
                    "warden_address"
                ]
            case "rangeserver":
                server_address = self.servers_config["range_server"][
                    "proto_server_addr"
                ]
            case _:
                server_address = self.servers_config[server]["proto_server_addr"]

        ip, port = server_address.split(":")
        start = time.time()
        while time.time() - start < timeout:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    if s.connect_ex((ip, int(port))) != 0:
                        return True
            except Exception as e:
                print(f"Error waiting for {server} to release port: {e}")
            time.sleep(0.5)
        return False

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
                    result = subprocess.run(
                        ["kill", "-9", str(self.pids[server])],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode != 0:
                        print(
                            f"Warning: Failed to kill {server} (PID {self.pids[server]}): {result.stderr}"
                        )
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
                self.wait_for_port_release(server)
                print(f"- Spinning up {server}")
                p = subprocess.Popen(
                    [
                        TARGET_RUN_CMD + server,
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


atomix_setup = AtomixSetup()
