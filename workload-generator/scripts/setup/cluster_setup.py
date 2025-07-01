from abc import ABC

import subprocess
import tempfile
import os
import concurrent.futures


def build_docker_images_script(images):
    script = ""
    for image in images:
        script += f"sudo docker build -t {image}:latest --target {image} --build-arg BUILD_TYPE=release .\n"
    return script


def run_dockers(images):
    script = ""
    # Run cassandra
    script += "sudo docker run -d -p 9042:9042 --name cassandra --ulimit nofile=100000:100000 cassandra:5.0\n"
    script += (
        "sudo docker exec -i cassandra cqlsh < schema/cassandra/atomix/keyspace.cql\n"
    )
    script += "sudo docker exec -i cassandra cqlsh -k atomix < schema/cassandra/atomix/schema.cql\n"

    for image in images:
        script += f"sudo docker run -d --name {image} {image}\n"

    return script


class Node(ABC):
    def __init__(self, config):
        self.config = config
        self.name = config["name"]
        self.address = config["address"]
        self.username = config["username"]
        self.branch = config["branch"]

    def setup(self):
        # Install docker
        script = ""
        script += """sudo apt-get update\n"""
        script += """sudo apt-get install ca-certificates curl\n"""
        script += """sudo install -m 0755 -d /etc/apt/keyrings\n"""
        script += """sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc\n"""
        script += """sudo chmod a+r /etc/apt/keyrings/docker.asc\n"""
        script += """echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null\n"""
        script += """sudo apt-get update\n"""
        script += """sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin\n"""

        # Git clone atomix
        script += """\
        if [ ! -d "$HOME/tmp/atomix" ]; then
            mkdir -p "$HOME/tmp" && cd "$HOME/tmp"
            git clone https://github.com/atomixdata/atomix.git
            cd atomix
                git checkout {self.branch}
        else
            cd "$HOME/tmp/atomix"
            git pull
        fi
        """
        return script

    def teardown(self):
        pass


class RemoteNode(Node):
    def __init__(self, config):
        super().__init__(config)

    def setup(self):
        script = super().setup()
        print(f"Setting up remote node {self.name}...")
        script += build_docker_images_script(self.config["images"])
        script += run_dockers(self.config["images"])
        script = "#!/bin/bash\nset -e\n" + script
        self.run_script(script)

    def run_script(self, script):
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write(script)
            tmp_path = tmp.name

        try:
            print(f"[{self.name}] Copying script to remote host...")
            subprocess.run(
                ["scp", tmp_path, f"{self.username}@{self.address}:/tmp/setup.sh"],
                check=True,
            )

            print(f"[{self.name}] Running script on remote host...")
            subprocess.run(
                ["ssh", f"{self.username}@{self.address}", "bash /tmp/setup.sh"],
                check=True,
            )
        finally:
            os.remove(tmp_path)


class LocalNode(Node):
    def __init__(self, config):
        super().__init__(config)

    def setup(self):
        print(f"Setting up local node {self.name}...")
        script = super().setup()
        script += build_docker_images_script(self.config["images"])
        script += run_dockers(self.config["images"])
        script = "#!/bin/bash\nset -e\n" + script
        self.run_script(script)

    def run_script(self, script):
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write(script)
            tmp_path = tmp.name

        try:
            print(f"[{self.name}] Running local setup script...")
            subprocess.run(["bash", tmp_path], check=True)
        finally:
            os.remove(tmp_path)


class ExperimentSetup:
    def __init__(self, nodes):
        self.nodes = nodes

    def setup(self):
        print(f"Setting up {len(self.nodes)} nodes...")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(node.setup) for node in self.nodes]
            concurrent.futures.wait(futures)

    def teardown(self):
        print(f"Tearing down {len(self.nodes)} nodes...")
        for node in self.nodes:
            node.teardown()


def main():
    username = "kelkost"
    branch = "expsremote"

    resolver_node_config = {
        "name": "resolver",
        "address": "128.105.146.3",
        "images": ["resolver"],
        "username": username,
        "branch": branch,
    }
    primary_node_config = {
        "name": "primary",
        "address": "127.0.0.1",
        "images": ["universe", "warden", "rangeserver", "frontend"],
        "username": username,
        "branch": branch,
    }
    secondary_node_config = {
        "name": "secondary",
        "address": "128.105.146.6",
        "images": ["universe", "warden", "rangeserver", "frontend"],
        "username": username,
        "branch": branch,
    }

    nodes = [
        RemoteNode(resolver_node_config),  # To be used for the resolver
        RemoteNode(primary_node_config),  # To be used for Atomix 2
        LocalNode(secondary_node_config),  # To be used for Atomix 1
    ]

    experiment_setup = ExperimentSetup(nodes)
    experiment_setup.setup()


if __name__ == "__main__":
    main()
