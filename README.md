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
* No coordinated omission
* Optional warmup iterations
* Configurable data-set size
* Configurable number of connections and threads
* Rate and parallelism limiters
* Beautiful text reports
* Can dump report in JSON
* Side-by-side comparison to the results of a saved run
* Error margin estimates 
* Statistical significance analysis 
    
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
$ ./target/release/latte -n 5000000 read  -d 1 -s 1 localhost -x baseline.json 
CONFIG =============================================================================================
                          ---------- This ---------- ---------- Other ----------    Change   
            Date        : Mon, 05 Oct 2020           Mon, 05 Oct 2020                             
            Time        : 17:37:58 +0200             17:25:50 +0200                               
           Label        :                                                                         
        Workload        : read                       read                                         
         Threads        :         1                           1                      +0.0%          
     Connections        :         1                           1                      +0.0%          
 Max parallelism   [req]:      1024                        1024                      +0.0%          
        Max rate [req/s]:                                                                         
          Warmup   [req]:         1                           1                      +0.0%          
      Iterations   [req]:   5000000                     5000000                      +0.0%          
        Sampling     [s]:       1.0                         1.0                      +0.0%          

LOG ================================================================================================
    Time  Throughput    ----------------------- Response times [ms]---------------------------------
     [s]     [req/s]       Min        25        50        75        90        95        99       Max
   1.000      156704      1.04      5.11      6.07      7.24      8.76     10.18     13.54     23.22
   2.000      155244      1.09      5.14      5.99      7.16      9.05     10.53     14.97     25.39
   3.000      153261      1.38      5.04      6.12      7.53      9.65     11.01     13.86     19.06
   4.000      159546      1.02      5.04      6.01      7.05      8.65     10.03     13.78     21.12
   5.000      158717      0.77      5.09      5.98      7.16      8.96     10.34     13.48     19.60
   6.000      154326      0.67      5.09      6.15      7.30      8.89     10.27     14.35     32.65
   7.000      157764      1.15      4.96      6.01      7.18      9.05     10.51     14.17     18.88
   8.000      157709      1.06      5.10      6.06      7.28      9.02     10.34     12.55     17.25
   9.000      145214      0.69      5.20      6.22      7.78     10.63     12.68     16.96     25.90
  10.000      143444      1.08      5.38      6.32      7.83     10.26     12.16     17.23     35.26
  11.000      156582      1.00      4.88      6.06      7.56      9.29     10.49     12.77     18.33
  12.000      152893      2.20      5.22      6.20      7.27      9.11     10.71     15.30     23.97
  13.000      153771      0.99      5.28      6.17      7.38      9.18     10.60     13.97     18.14
  14.000      154055      1.01      5.24      6.18      7.39      9.19     10.69     13.34     24.85
  15.000      153159      0.54      5.11      6.14      7.40      9.26     10.74     16.33     28.35
  16.000      153898      0.68      5.34      6.30      7.42      8.86     10.04     12.69     18.88
  17.000      152783      1.70      5.37      6.26      7.30      9.06     10.46     13.17     22.99
  18.000      149409      0.92      5.30      6.33      7.59      9.52     10.98     15.85     23.74
  19.000      144543      0.72      5.36      6.41      7.96     10.23     11.94     15.62     31.31
  20.000      150754      0.77      5.02      6.26      7.74      9.81     11.22     14.24     26.78
  21.000      149889      0.82      5.20      6.26      7.61      9.76     11.57     14.77     24.14
  22.000      151592      0.70      5.15      6.23      7.59      9.69     11.12     13.36     20.54
  23.000      152598      0.59      5.22      6.24      7.51      9.10     10.34     13.11     19.15
  24.000      149039      1.97      5.31      6.38      7.72      9.52     10.90     14.42     26.56
  25.000      151437      1.02      5.41      6.34      7.49      9.11     10.47     12.90     20.11
  26.000      142431      0.88      5.39      6.54      8.01     10.01     11.85     19.07     24.56
  27.000      143576      1.17      5.41      6.45      8.02     10.34     11.61     14.85     19.26
  28.000      149446      0.89      5.10      6.44      7.89      9.53     10.91     13.69     20.06
  29.000      149372      1.77      5.36      6.34      7.59      9.49     10.61     13.87     21.85
  30.000      151069      0.73      5.10      6.32      7.76      9.49     10.89     14.34     22.77
  31.000      150991      0.65      5.28      6.22      7.52      9.26     11.15     14.58     21.71
  32.000      149848      1.07      5.36      6.35      7.56      9.40     10.97     13.94     23.90

SUMMARY STATS ======================================================================================
                          ---------- This ----------- ---------- Other ----------   Change   Signif.
         Elapsed     [s]:    32.975                      34.475                      -4.4%          
        CPU time     [s]:    51.278          ( 19.4%)    51.095          ( 18.5%)    +0.4%          
       Completed   [req]:   5000000          (100.0%)   5000000          (100.0%)    +0.0%          
          Errors   [req]:         0          (  0.0%)         0          (  0.0%)                   
      Partitions        :   5000000                     5000000                      +0.0%          
            Rows        :   5000000                     5000000                      +0.0%          
     Parallelism   [req]:    1005.4          ( 98.2%)    1006.0          ( 98.2%)    -0.1%          
      Throughput [req/s]:    151632 ± 2600               145032 ± 2974               +4.6%     [***]
 Mean resp. time    [ms]:      6.64 ± 0.12                 6.95 ± 0.15               -4.5%     [***]

THROUGHPUT [req/s] =================================================================================
                          ---------- This ----------- ---------- Other ----------   Change   
             Min        :    142431                      130450                      +9.2%          
               1        :    142431                      130450                      +9.2%          
               2        :    142431                      130450                      +9.2%          
               5        :    143444                      136309                      +5.2%          
              10        :    144543                      139346                      +3.7%          
              25        :    149409                      142308                      +5.0%          
              50        :    151592                      144915                      +4.6%          
              75        :    154055                      148393                      +3.8%          
              90        :    157709                      151960                      +3.8%          
              95        :    158717                      153884                      +3.1%          
             Max        :    159546                      156008                      +2.3%          

RESPONSE TIMES [ms] ================================================================================
                          ---------- This ----------- ---------- Other ----------   Change   Signif.
             Min        :      0.54 ± 0.23                 0.47 ± 0.34              +13.3%     [   ]
              25        :      5.20 ± 0.08                 5.28 ± 0.09               -1.6%     [   ]
              50        :      6.22 ± 0.08                 6.54 ± 0.12               -4.8%     [***]
              75        :      7.52 ± 0.15                 8.10 ± 0.19               -7.2%     [***]
              90        :      9.42 ± 0.28                 9.98 ± 0.31               -5.7%     [***]
              95        :     10.91 ± 0.37                11.38 ± 0.42               -4.1%     [*  ]
              98        :     12.83 ± 0.58                13.31 ± 0.60               -3.6%     [   ]
              99        :     14.35 ± 0.84                14.97 ± 0.83               -4.1%     [   ]
            99.9        :     21.62 ± 2.30                23.34 ± 2.07               -7.3%     [   ]
           99.99        :     30.63 ± 2.41                28.42 ± 2.23               +7.8%     [   ]
             Max        :     35.24 ± 2.54                30.55 ± 2.19              +15.4%     [   ]

RESPONSE TIME DISTRIBUTION =========================================================================
Percentile    Resp. time [ms]  ----------------------------- Count ---------------------------------
   0.00000          0.54               0   
   0.00074          0.67              37   
   0.00416          0.84             171   
   0.01730          1.04             657   
   0.05868          1.31            2069   
   0.13072          1.63            3602   
   0.25006          2.04            5967   
   0.47914          2.55           11454   
   1.13020          3.19           32553   
   5.21920          3.99          204450   ****
  21.02468          4.99          790274   ***************
  50.39588          6.24         1468560   *****************************
  78.31782          7.80         1396097   ***************************
  91.42282          9.75          655250   *************
  97.24988         12.19          291353   *****
  99.29320         15.23          102166   **
  99.76942         19.04           23811   
  99.95016         23.80            9037   
  99.98684         29.75            1834   
 100.00000         37.19             658    
```

