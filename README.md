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
$ latte read -n 4000000 -d 1 localhost 
CONFIG ===================================================================================
               Time: Wed, 30 Sep 2020 15:34:12 +0200
              Label: 
           Workload: read
            Threads:           1
  Total connections:           1
         Rate limit:           - req/s
  Concurrency limit:        1024 req
  Warmup iterations:           1 req
Measured iterations:     4000000 req
           Sampling:         1.0 s

LOG ======================================================================================
                      ----------------------- Response times [ms]-------------------------
Time [s]  Throughput       Min        25        50        75        90        99       Max
   1.000      157842      0.86      5.24      6.09      7.22      8.52     11.14     23.91
   2.000      159696      2.05      5.30      6.11      7.02      8.19     10.22     15.91
   3.000      156986      0.72      5.20      6.16      7.22      8.45     11.51     23.92
   4.000      158408      1.96      5.04      6.04      7.36      8.89     10.97     21.19
   5.000      159301      1.02      5.03      6.10      7.23      8.69     11.06     21.84
   6.000      156801      0.79      5.04      6.14      7.34      8.58     12.01     25.71
   7.000      158437      1.16      5.19      6.10      7.15      8.50     11.37     20.74
   8.000      157563      1.80      5.07      6.06      7.17      8.69     12.20     23.71
   9.000      159230      0.99      5.21      6.09      7.14      8.59     11.13     22.40
  10.000      160398      0.77      4.88      5.95      7.30      8.81     11.40     22.27
  11.000      156318      1.93      5.09      6.04      7.28      8.82     12.22     24.23
  12.000      151943      0.61      5.23      6.29      7.57      9.41     12.37     33.39
  13.000      158312      0.83      5.12      6.06      7.18      8.82     11.62     26.90
  14.000      159552      0.81      4.99      5.98      7.12      8.66     11.85     23.84
  15.000      160504      0.83      4.93      6.04      7.16      8.71     11.30     24.00
  16.000      155892      1.61      5.16      6.11      7.25      8.78     12.50     21.70
  17.000      157108      1.50      5.12      6.10      7.31      8.85     11.42     30.65
  18.000      161212      0.44      4.88      5.94      7.23      8.83     11.75     26.36
  19.000      153803      2.30      5.32      6.19      7.29      8.73     11.84     33.91
  20.000      154925      1.95      5.18      6.12      7.42      9.24     11.60     20.45
  21.000      160086      0.60      4.96      5.99      7.30      8.73     11.20     20.65
  22.000      159015      1.32      5.12      6.03      7.24      8.60     11.14     22.10
  23.000      159891      2.34      5.09      6.02      7.14      8.58     11.08     21.83
  24.000      158364      1.05      5.14      6.07      7.06      8.66     11.67     24.01
  25.000      157773      2.77      5.16      6.13      7.31      8.53     10.68     18.40

SUMMARY STATS ============================================================================
            Elapsed:      25.317          s
           CPU time:      39.585          s         19.5%
          Completed:     4000000          req      100.0%
             Errors:           0          req        0.0%
         Partitions:     4000000
               Rows:     4000000
    Mean throughput:      157994 ± 1440   req/s      0.0%
    Mean resp. time:        6.38 ± 0.06   ms
   Mean concurrency:     1005.39          req       98.2%

THROUGHPUT ===============================================================================
                Min:      151943          req/s
                  1:      151943          req/s
                  5:      153803          req/s
                 25:      156986          req/s
                 50:      158363          req/s
                 75:      159552          req/s
                 95:      160504          req/s
                Max:      161212          req/s

RESPONSE TIMES ===========================================================================
                Min:        0.44 ± 0.43   ms
                 25:        5.11 ± 0.08   ms
                 50:        6.08 ± 0.05   ms
                 75:        7.24 ± 0.08   ms
                 90:        8.71 ± 0.16   ms
                 95:        9.87 ± 0.22   ms
                 98:       11.48 ± 0.35   ms
                 99:       12.80 ± 0.67   ms
               99.9:       20.91 ± 2.59   ms
              99.99:       27.12 ± 2.83   ms
                Max:       33.91 ± 2.72   ms

RESPONSE TIME DISTRIBUTION ==============================================================
Percentile   Resp. time      Count
   0.00000      0.44 ms          0   
   0.00015      0.55 ms          6   
   0.00063      0.69 ms         19   
   0.00230      0.86 ms         67   
   0.00950      1.07 ms        288   
   0.02675      1.34 ms        690   
   0.06703      1.68 ms       1611   
   0.15348      2.10 ms       3458   
   0.39300      2.62 ms       9581   
   1.22948      3.28 ms      33459   
   7.09888      4.10 ms     234776   *****
  25.31890      5.12 ms     728801   ******************
  58.49018      6.40 ms    1326851   *********************************
  84.46230      8.00 ms    1038885   *************************
  95.38570     10.00 ms     436936   **********
  98.85332     12.50 ms     138705   ***
  99.62380     15.63 ms      30819   
  99.84132     19.54 ms       8701   
  99.97888     24.42 ms       5502   
  99.99882     30.53 ms        798   
 100.00000     38.16 ms         47   
```

