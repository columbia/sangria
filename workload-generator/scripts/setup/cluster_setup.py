from abc import ABC

import subprocess
import tempfile
import os

def build_docker_images_script(images):
    for image in images:
        script += f"docker build -t {image} . --target {image} --build-arg BUILD_TYPE=release\n"
    return script


class Node(ABC):
    def __init__(self, node_config):
        self.node_config = node_config

    def setup(self):
        # Install docker
        script = ""
        script += "sudo apt-get update\n"
        script += "sudo apt-get install ca-certificates curl\n"
        script += "sudo install -m 0755 -d /etc/apt/keyrings\n"
        script += "sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc\n"
        script += "sudo chmod a+r /etc/apt/keyrings/docker.asc\n"
        script += "echo   \"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \n"
        script += "$(. /etc/os-release && echo \"${UBUNTU_CODENAME:-$VERSION_CODENAME}\") stable\" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null\n"
        script += "sudo apt-get update\n"
        script += "sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin\n"
        return script

    def teardown(self):
        pass



class RemoteNode(Node):
    def __init__(self, node_config):
        super().__init__(node_config)

    def setup(self):
        script = super().setup()
        # script += build_docker_images_script(self.node_config["images"])
        script = "#!/bin/bash\nset -e\n" + script
        self.run_script(script)

    def run_script(self, script):
        address = self.node_config["address"]
        name = self.node_config["name"]
        
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write(script)
            tmp_path = tmp.name

        try:
            print(f"[{name}] Copying script to remote host...")
            subprocess.run(["scp", tmp_path, f"{address}:/tmp/setup.sh"], check=True)

            print(f"[{name}] Running script on remote host...")
            subprocess.run(["ssh", address, "bash /tmp/setup.sh"], check=True)
        finally:
            os.remove(tmp_path)


class LocalNode(Node):
    def __init__(self, node_config):
        super().__init__(node_config)

    def setup(self):
        script = super().setup()
        # script += build_docker_images_script(self.node_config["images"])
        script = "#!/bin/bash\nset -e\n" + script
        self.run_script(script)

    def run_script(self, script):
        name = self.node_config["name"]

        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write(script)
            tmp_path = tmp.name

        try:
            print(f"[{name}] Running local setup script...")
            subprocess.run(["bash", tmp_path], check=True)
        finally:
            os.remove(tmp_path)


class ExperimentSetup:
    def __init__(self, setup_config):
        self.nodes = setup_config["nodes"]
        
    def setup(self):
        for node in self.nodes:
            node.setup()

    def teardown(self):
        for node in self.nodes:
            node.teardown()



def main():

    resolver_node_config = {
        "name": "resolver",
        "address": "128.105.144.143",
        "images": ["atomix-resolver"],
    },
    # main_workload_node_config = {
    #     "name": "main-workload",
    #     "address": "128.105.146.6",
    #     "images": ["atomix-universe", "atomix-warden", "atomix-rangeserver", "atomix-frontend"],
    # },
    # secondary_workload_node_config = {
    #     "name": "secondary-workload",
    #     "address": "128.105.146.3",
    #     "images": ["atomix-universe", "atomix-warden", "atomix-rangeserver", "atomix-frontend"],
    # }

    nodes = [
        RemoteNode(resolver_node_config),               # To be used for the resolver
        # RemoteNode(main_workload_node_config),          # To be used for Atomix 2
        # LocalNode(secondary_workload_node_config),      # To be used for Atomix 1
    ]

    setup_config = {
        "nodes": nodes,
    }
    experiment_setup = ExperimentSetup(setup_config)
    experiment_setup.setup()

if __name__ == "__main__":
    main()