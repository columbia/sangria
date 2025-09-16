# Sangria

An adaptive commit protocol for distributed transactions.


## Installation guide

```bash
sudo apt-get update && sudo apt-get install -y \
    git \
    cmake \
    make \
    clang


git clone https://github.com/google/flatbuffers.git && \
cd flatbuffers && \
git checkout v23.5.26 && \
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release && \
sudo make -j$(nproc) && \
sudo make install

cd ../

sudo apt-get -qy update && \
    sudo apt-get -qy install apt-transport-https

sudo apt-get -qy update && \
sudo apt-get -qy install dos2unix openssh-server pwgen
```

### install rust
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

### install docker
```bash
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo   "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
$(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

### install protobuf
```bash
curl -OL https://github.com/google/protobuf/releases/download/v30.2/protoc-30.2-linux-x86_64.zip
unzip protoc-30.2-linux-x86_64.zip -d protoc3

sudo mv protoc3/bin/* /usr/local/bin/
sudo mv protoc3/include/* /usr/local/include/
```

### set cassandra
```bash
sudo docker run -d -p 9042:9042 --name cassandra --ulimit nofile=100000:100000 cassandra:5.0
cd atomix
sudo docker exec -i cassandra cqlsh < schema/cassandra/atomix/keyspace.cql
sudo docker exec -i cassandra cqlsh -k atomix < schema/cassandra/atomix/schema.cql
```

As you run stuff sometimes you may want to clean up cassandra to reduce its load and make a clean start.
Use:
```bash 
sudo docker exec -i cassandra cqlsh -e "TRUNCATE atomix.range_map; TRUNCATE atomix.epoch; TRUNCATE atomix.range_leases; TRUNCATE atomix.records; TRUNCATE atomix.wal; TRUNCATE atomix.transactions; TRUNCATE atomix.keyspaces;"
```


## Run guide

check out these script files to see how I automate the execution of the Sangria experiments. 

- here you can find (with some effort) the instrutions to build and run the servers you need to run sangria: `workload-generator/scripts/atomix_setup.py`
always make sure you build with the --release flag

- `workload-generator/scripts/run_experiments.py` this is a script that I use to run all experiments automatically. each funtion (calling them in `main` - uncommenting the one I want every time) corresponds to a specific experiment. 
as you can see I'm using Ray tune which is an amazing tool that lets you define 
a grid of different configuration parameters for each experiment and then runs all different configurations one after the other sequentially.
Then results are logged in a specific directory and processed and plotted from there.

- `workload-generator/scripts/ray_task.py`
this is the function called for each instance of an experiment. e.g. for the experiment `tradeoff_contention_vs_resolver_capacity_experiment` ray runs different combinations of configs as specified in run_experiments.py. For each such combination the run_workload in ray_task.py function is called. The python code in the scripts is generally horrible and made in a rush. but if you spend some time reading/understanding it you ll familiarize yourself on how to set up and run the experiments how and in which order to spin up the servers etc.

focus on the run_experiments.py / atomix_setup.py / ray_task.py files. understand how ray works and maybe as a first step try to reproduce the first experiment/function in the run_experiments.py file.


You'll need 5 big servers to setup sangria. universe / warden / rangeserver / resolver / frontend. Try to spin them in that order to avoid errors if you run something manually. Running a workload manually instead of using the ray automation is useful to debug stuff especially if you enable logs/printing messages.

Inside the workload-generator crate you'll find my implementation for a custom/synthetic workload generator. see how I call it in the automated scripts. 

I call it twice if you notice. First call is to initiate a workload of fake transactions whose purpose is to just overload the resolver and limit its capacity. the second workload is the main one whose metrics we get in the end. again the code in the scripts is horrible but it's worth trying to understand it.

once you get to a point where you have a running setup where you understand how the servers work, how to manually run one workload (i.e. no ray in which case you'll see that you need to be careful about the order in which you spin the servers - when you reset them or when you clean up cassandra between runs) or how to run multiple configs for an experiment sequentially automaticall with ray.. then and only then you should start looking into the Resolver code - understand how dependency resolution is implemented and then inside the rangeserver/impl.rs to see the implementation of strict/pipelined/adaptive 2pc etc.


### advice

- try to isolate the parts of the codebase that you'll need. 
- understand how those parts work deeply even if it feels cumbersome - you'll find it useful eventually.
- set clear intermediate goals every step of the way.
e.g. 1) learn how to run the servers 2) learn how to run the experiments using the scripts 3) run manually an experiment using the X config files. 
- read as much code as possible - the code is the ultimate source of truth - have a holistic view of the codebase and then concentrate only on those portions you need -- and dig deeply into those. 
- catching up with all that without the constant help of me (sorry but with work/life and all I don't expect to always be immediately responsive but will try to be decent) is gonna be overwhelming. navigating that chaos / making sense of everything mainly on your own and keeping your coolness is part of research and the phd process if you ever wanna go down that path :p so see it as a test drive
