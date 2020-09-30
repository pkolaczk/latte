# Latency Tester for Apache Cassandra

A tiny native program that issues concurrent CQL queries to an Apache Cassandra
cluster and measures throughput and response times. 

## Why Yet Another Benchmarking Program?

Contrary to `cassandra-stress` or `nosql-bench`, 
`latte` has been written in Rust and uses DataStax C++ Driver for Cassandra. 
This enables it to achieve superior performance and predictability: 

* Over **50x lower memory footprint** (typically below 20 MB instead of 1+ GB)  
* Over **6x better CPU efficiency**. This means you can test larger clusters or reduce the 
  number of client machines.  
* Can run on one of the nodes without significant performance impact on the server.
  In this setup, throughput levels achieved by `latte` tests are typically over 2x higher than 
  when using the other tools, because almost all the processing power is available to the server, instead of
  half of it being consumed by the benchmarking tool.
* No client code warmup needed. The client code works with maximum 
  performance from the first iteration. If the server is already warmed-up,
  this means much shorter tests and quicker iteration. This also allows for accurate 
  measurement of warmup effects happening on the benchmarked server(s). 
* No GC pauses nor HotSpot recompilation happening in the middle of the test. 
  We want to measure hiccups of the server, not the benchmarking tool. 
    
## Limitations
This is work-in-progress.
* Workload selection is currently limited to hardcoded read and write workloads dealing with tiny rows
* No verification of received data - this is a benchmarking tool, not a testing tool.

## Installation
1. [Install Datastax C++ Driver 2.15](https://docs.datastax.com/en/developer/cpp-driver/2.15/topics/installation/) 
   with development packages
2. [Install Rust toolchain](https://rustup.rs/)
3. Run `cargo install --git https://github.com/pkolaczk/latte`

## Usage
1. Start a Cassandra cluster somewhere (can be a local node)
2. Run `latte <workload> <node address>`, where `<workload>` can be `read` or `write`

Run `latte -h` to display help with the available options.

## Example Report
```
CONFIG -----------------------------------------
           Threads:           2
 Total connections:           2
        Rate limit:         disabled
 Concurrency limit:        1024 reqs

SUMMARY ----------------------------------------
           Elapsed:       0.670 s
          CPU time:       1.239 s        185.0%
         Completed:      100000 reqs     100.0%
            Errors:           0 reqs       0.0%
        Throughput:    149299.5 req/s      0.0%
  Avg. concurrency:       998.6 reqs      97.5%

RESPONSE TIMES ---------------------------------
               Min:       0.352 ms
                25:       4.123 ms
                50:       5.831 ms
                75:       8.447 ms
                90:      11.727 ms
                95:      13.679 ms
                99:      18.527 ms
              99.9:      24.367 ms
             99.99:      26.751 ms
               Max:      26.959 ms

DETAILED HISTOGRAM -----------------------------
Percentile   Resp. time      Count
  0.00         0.351 ms          0   |
  0.01         0.456 ms          8   |
  0.02         0.593 ms         12   |
  0.06         0.772 ms         37   |
  0.17         1.004 ms        109   |
  0.43         1.305 ms        264   |
  1.44         1.698 ms       1011   |*
  4.18         2.207 ms       2736   |**
  9.62         2.870 ms       5446   |*****
 19.33         3.731 ms       9705   |*********
 35.52         4.851 ms      16193   |****************
 55.35         6.307 ms      19833   |*******************
 73.53         8.199 ms      18173   |******************
 86.57        10.660 ms      13045   |*************
 95.31        13.858 ms       8742   |********
 98.87        18.016 ms       3553   |***
 99.82        23.421 ms        951   |
100.00        30.448 ms        182   |
```

