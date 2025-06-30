import json
import os
import subprocess
import time
import socket
from utils import *
import psutil
import signal

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

    def get_server_addresses(self, server: str):
        match server:
            case "warden":
                return [self.servers_config["regions"]["test-region"][
                    "warden_address"
                ]]
            case "rangeserver":
                return [self.servers_config["range_server"][
                    "proto_server_addr"
                ], self.servers_config["range_server"][
                    "fast_network_addr"
                ]]
            case "frontend":
                return [self.servers_config["frontend"][
                    "proto_server_addr"
                ], self.servers_config["frontend"][
                    "fast_network_addr"
                ]]
            case "resolver":
                return [self.servers_config["resolver"][
                    "proto_server_addr"
                ], self.servers_config["resolver"][
                    "fast_network_addr"
                ]]
            case "universe":
                return [self.servers_config["universe"][
                    "proto_server_addr"
                ]]
            case _:
                return []

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
                print(f"- Spinning up {server}")
                try:
                    proc = self.start_server_with_retry(server, max_retries=10000, retry_delay=4.0)
                except Exception:
                    print("Failed to launch server after retries")
                self.pids[server] = proc.pid
                time.sleep(1)
            except Exception as e:
                print(f"Error spinning up {server}: {e}")
                self.kill_servers(servers=servers)
                exit(1)

    def start_server_with_retry(self, server: str,
                                max_retries: int = 5,
                                retry_delay: float = 1.0):
        for attempt in range(1, max_retries + 1):
            try:
                print(f"[Attempt {attempt}] Spinning up {server}…")
                log_path = os.path.join(ROOT_DIR, f"log-{server}.txt")
                if os.path.exists(log_path):
                    os.remove(log_path)

                with open(log_path, "w") as log_file:
                    p = subprocess.Popen(
                        [TARGET_RUN_CMD + server,
                        "--config", str(RAY_SERVERS_CONFIG_PATH)],
                        cwd=ROOT_DIR,
                        start_new_session=True,
                        text=True,
                        stdout=log_file,
                        stderr=log_file,
                        env={**os.environ, "RUST_LOG": "error"},
                    )
                time.sleep(1)
                if p.poll() is not None:
                    server_addresses = self.get_server_addresses(server)
                    for server_address in server_addresses:
                        _, port = server_address.split(":")
                        for conn in psutil.net_connections(kind="inet"):
                            if conn.pid is not None and conn.laddr.port == int(port):
                                print(f"Killing process {conn.pid} listening on port {port}")
                                os.kill(conn.pid, signal.SIGKILL)
                                break
                    raise RuntimeError(f"{server} exited with code {p.returncode}")

                print(f"{server} started (pid={p.pid})")
                return p

            except Exception as e:
                print(f"Error starting {server}: {e!r}")
                if attempt < max_retries:
                    print(f" → retrying in {retry_delay}s…")
                    time.sleep(retry_delay)
                else:
                    print(f" → gave up after {max_retries} attempts")
                    raise



atomix_setup = AtomixSetup()
