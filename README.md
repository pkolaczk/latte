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

## Features
* Automatic creation of schema and data-set population
* Accurate measurement of throughput and response times
* Optional warmup iterations
* Configurable data-set size
* Configurable number of connections and threads
* Rate and parallelism limiters
* Beautiful text reports
* Can dump report in JSON
* Side-by-side comparison to the results of a saved run
* Error margin estimates  
    
## Limitations 
This is work-in-progress.
* Workload selection is currently limited to hardcoded read and write workloads dealing with tiny rows
* No analysis of statistical significance of differences
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
$ latte read -n 1000000 -d 1 localhost -x baseline.json
  CONFIG ==============================================================================================
                               ---------- This ---------- ---------- Other ----------    Change   
                 Date        : Fri, 02 Oct 2020           Fri, 02 Oct 2020                   
                 Time        : 18:42:28 +0200             18:24:44 +0200                     
                Label        :                                                               
             Workload        : read                       read                               
              Threads        :         1                           1                      +0.0%
          Connections        :         1                           1                      +0.0%
    Parallelism limit   [req]:      1024                        1024                      +0.0%
           Rate limit [req/s]:                                                               
    Warmup iterations   [req]:         1                           1                      +0.0%
  Measured iterations   [req]:   1000000                     1000000                      +0.0%
             Sampling     [s]:       1.0                         1.0                      +0.0%
  
  LOG =================================================================================================
                           ----------------------- Response times [ms]-------------------------
  Time [s]  Throughput          Min        25        50        75        90        99       Max
     1.000      156228         0.88      5.07      6.13      7.52      8.84     12.00     17.90
     2.000      153400         2.04      5.26      6.22      7.44      8.80     12.62     26.41
     3.000      151364         0.93      5.41      6.29      7.46      9.22     12.56     15.69
     4.000      155747         2.59      5.32      6.22      7.28      8.56     12.61     16.58
     5.000      154694         1.01      5.16      6.10      7.26      9.14     15.25     21.66
     6.000      155099         1.36      5.32      6.25      7.41      8.78     12.29     17.42
  
  SUMMARY STATS =======================================================================================
                               ---------- This ---------- ---------- Other ----------    Change   
              Elapsed     [s]:     6.469                       6.664                      -2.9%
             CPU time     [s]:    10.083          ( 19.5%)    10.141          ( 19.0%)    -0.6%
            Completed   [req]:   1000000          (100.0%)   1000000          (100.0%)    +0.0%
               Errors   [req]:         0          (  0.0%)         0          (  0.0%)         
           Partitions        :   1000000                     1000000                      +0.0%
                 Rows        :   1000000                     1000000                      +0.0%
          Parallelism   [req]:    1005.3          ( 98.2%)    1005.5          ( 98.2%)    -0.0%
           Throughput [req/s]:    154586 ± 2399               150057 ± 4795               +3.0%
      Mean resp. time    [ms]:      6.51 ± 0.10                 6.71 ± 0.21               -3.0%
  
  THROUGHPUT [req/s] ==================================================================================
                               ---------- This ---------- ---------- Other ----------    Change   
                  Min        :    151364                      143959                      +5.1%
                    1        :    151364                      143959                      +5.1%
                    2        :    151364                      143959                      +5.1%
                    5        :    151364                      143959                      +5.1%
                   10        :    151364                      143959                      +5.1%
                   25        :    153400                      147544                      +4.0%
                   50        :    154694                      150583                      +2.7%
                   75        :    155746                      152052                      +2.4%
                   90        :    156228                      153638                      +1.7%
                   95        :    156228                      153638                      +1.7%
                  Max        :    156228                      153638                      +1.7%
  
  RESPONSE TIMES [ms] =================================================================================
                               ---------- This ---------- ---------- Other ----------    Change   
                  Min        :      0.88 ± 0.94                 0.80 ± 0.79               +9.5%
                   25        :      5.27 ± 0.17                 5.22 ± 0.21               +1.1%
                   50        :      6.21 ± 0.10                 6.29 ± 0.16               -1.3%
                   75        :      7.39 ± 0.14                 7.70 ± 0.29               -4.1%
                   90        :      8.84 ± 0.33                 9.60 ± 0.42               -7.9%
                   95        :      9.94 ± 0.58                11.02 ± 0.57               -9.8%
                   98        :     11.45 ± 0.97                12.91 ± 1.06              -11.3%
                   99        :     12.62 ± 1.59                14.18 ± 1.80              -11.0%
                 99.9        :     20.76 ± 5.87                18.91 ± 2.90               +9.8%
                99.99        :     26.04 ± 5.63                20.88 ± 2.93              +24.7%
                  Max        :     26.41 ± 5.44                25.21 ± 4.16               +4.8%
  
  RESPONSE TIME DISTRIBUTION ==========================================================================
  Percentile     Resp. time [ms]  ----------------------------- Count ---------------------------------
     0.00000          0.88               0   
     0.00160          1.10              16   
     0.00460          1.37              30   
     0.01270          1.71              81   
     0.05430          2.14             416   
     0.17460          2.67            1203   
     0.71010          3.34            5355   
     6.11240          4.18           54023   *****
    24.09770          5.23          179853   *****************
    58.31260          6.53          342149   **********************************
    84.60030          8.17          262877   **************************
    95.74320         10.21          111429   ***********
    99.07790         12.76           33347   ***
    99.75000         15.95            6721   
    99.89120         19.94            1412   
    99.94860         24.92             574   
   100.00000         31.16             514   

```

