# abstract

nDPId is a set of daemons and tools to capture, process and classify network flows.
It's only dependencies (besides a half-way modern c library and POSIX threads) are libnDPI and libpcap.

The core daemon nDPId uses `pthread` but does use slow mutexes.
Instead synchronization is achieved by a packet distribution mechanism.
To balance all workload to all threads (more or less) equally a hash value is calculated using the 5-tuple.
This value serves as unique identifier for the processing thread. Multithreaded packet processing has to be flow-stable.

nDPId uses libnDPI's JSON serialization to produce meaningful JSON output which it then sends to the nDPIsrvd for distribution.
High level applications can connect to nDPIsrvd to get the latest flow/packet events from nDPId.

TODO: Provide some sort of authentication for connecting distributor clients (somehow very critical).

# architecture

This project uses some kind of microservice architecture.

```text
_______________________                                         __________________________
|      producer       |                                         |        consumer        |
|---------------------|      _____________________________      |------------------------|
|                     |      |        nDPIsrvd           |      |                        |
| nDPId --- Thread 1 >| ---> |>           |             <| <--- |< example/c-json-stdout |
|        `- Thread 2 >| ---> |> collector | distributor <| <--- |< example/py-flow-info  |
|        `- Thread N >| ---> |>           |             <| <--- |                        |
|_____________________|  ^   |____________|______________|   ^  |________________________|
                         |                                   |                            
                         `- connect to UNIX socket           `- connect to TCP socket     
```

# JSON TCP protocol

All JSON strings sent need to be in the following format:
```text
15{"key":"value"}
```
where `15` describes the length of a **complete** JSON string.

TODO: Describe data format via JSON schema.

# build

To get an overview over all build options, run:
```shell
make help
```

To build nDPId and nDPIsrvd, run:
```shell
make all
```

To build nDPId and nDPIsrvd with sanitizer, debug mode enabled and a custom/not-your-distro libnDPI, run:
```shell
make ENABLE_DEBUG=yes ENABLE_SANITIZER=yes CUSTOM_LIBNDPI=[path-to-libndpi].[a|so] all
```

To build nDPId and nDPIsrvd and examples, run:
```shell
make all examples
```

# run

Daemon mode:
```shell
./nDPIsrvd -d
./nDPId -d
```

And why not a simple flow-info example:
```shell
./examples/py-flow-info/flow-info.py
```