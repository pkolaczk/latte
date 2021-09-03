# Latency Tester for Apache Cassandra

Issues concurrent CQL queries to an Apache Cassandra cluster and measures throughput and response times.

## Why Yet Another Benchmarking Program?

Contrary to `cassandra-stress` and `nosqlbench`, Latte has been written in Rust and uses DataStax C++ Driver for
Cassandra.  
It has a fully asynchronous, thread-per-core workload execution engine. This enables it to achieve superior performance
and predictability:

* Over 50x lower memory footprint (typically below 20 MB instead of 1+ GB)
* Over 6x better CPU efficiency. This means you can test larger clusters or reduce the number of client machines.
* Can run on one of the nodes without significant performance impact on the server. In this setup, throughput levels
  achieved by `latte` tests are typically over 2x higher than when using the other tools, because almost all the
  processing power is available to the server, instead of half of it being consumed by the benchmarking tool.
* No client code warmup needed. The client code works with maximum performance from the first iteration. If the server
  is already warmed-up, this means much shorter tests and quicker iteration. This also allows for accurate measurement
  of warmup effects happening on the benchmarked server(s).
* No GC pauses nor HotSpot recompilation happening in the middle of the test. We want to measure hiccups of the server,
  not the benchmarking tool.

Workloads for Latte are fully customizable with a modern embedded scripting language [Rune](https://rune-rs.github.io/)
.  
Rune offers syntax and features similar to Rust, albeit with dynamic typing and easier memory management.

## Features

* Custom workloads with a powerful scripting engine
* Asynchronous CQL queries
* Workload parameterization
* Accurate measurement of throughput and response times with error margins
* No coordinated omission
* Configurable number of connections and threads
* Rate and parallelism limiters
* Progress bars
* Beautiful text reports
* Can dump report in JSON
* Side-by-side comparison of two runs
* Statistical significance analysis of differences corrected for auto-correlation

## Limitations

* Binding some CQL data types is not yet supported, e.g. user defined types, maps or integer types smaller than 64-bit.
* The set of data generating functions is tiny and will be extended soon.

## Installation

1. [Install Datastax C++ Driver 2.15](https://docs.datastax.com/en/developer/cpp-driver/2.15/topics/installation/)
   with development packages
2. [Install Rust toolchain](https://rustup.rs/)
3. Run `cargo install --git https://github.com/pkolaczk/latte`

## Usage

1. Start a Cassandra cluster somewhere (can be a local node).
2. Run `latte <workload> [<node address>]`, where `<workload>` is the path to the workload script. You can find example
   workloads in the `workloads` folder.

Run `latte -h` to display help with the available options.

## Workloads

Workloads are defined as Rune scripts. A workload script defines a set of public functions that Latte calls
automatically. A minimum viable workload script must define at least a single public async function `run` with two
arguments:

- `ctx` – session context that provides the access to Cassandra
- `i` – current unique iteration number of a 64-bit integer type, starting at 0

The following script would benchmark querying the `system.local` table:

```rust
pub async fn run(ctx, i) {
  ctx.execute("SELECT cluster_name FROM system.local LIMIT 1").await
}
```

Instance functions on `ctx` are asynchronous, so you should call `await` on them.

### Schema creation

You can create your own keyspaces and tables in the `schema` function:

```rust
pub async fn schema(ctx) {
  ctx.execute("CREATE KEYSPACE IF NOT EXISTS test \
                 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").await?;
  ctx.execute("CREATE TABLE IF NOT EXISTS test.test(id bigint, data varchar").await?;
}
```

### Prepared statements

Calling `ctx.execute` is not optimal, because it doesn't use prepared statements. You can prepare statements and
register them on the context object in the `prepare`
function:

```rust
const INSERT = "my_insert";
const SELECT = "my_select";

pub async fn prepare(ctx) {
  ctx.prepare(INSERT, "INSERT INTO test.test(id, data) VALUES (?, ?)").await?;
  ctx.prepare(SELECT, "SELECT * FROM test.test WHERE id = ?").await?;
}

pub async fn run(ctx, i) {
  ctx.execute_prepared(SELECT, [i]).await
}
```

### Populating the database

Read queries are more interesting when they return non-empty result sets. To load data into tables before each run of
the benchmark, define the `load` function and the `LOAD_COUNT` constant that will tell Latte the number of times `load`
must be called:

```rust
pub const LOAD_COUNT = 1000; // invoke load for i in 0..1000

pub async fn load(ctx, i) {
  ctx.execute_prepared(INSERT, [i, "Lorem ipsum dolor sit amet"]).await
}
```

We also recommend defining the `erase` function to erase the data before loading so that each benchmark run starts from
the same initial state:

```rust
pub async fn erase(ctx) {
  ctx.execute("TRUNCATE TABLE test.test").await
}
```

If needed, you can skip the loading phase by passing `--no-load` command line flag.

### Generating data

Latte comes with a library of data generating functions. They are accessible in the `latte` crate. Typically, those
functions accept an integer `i` iteration number, so you can generate consistent numbers. The data generating functions
are pure, i.e. invoking them multiple times with the same parameters yields always the same results.

- `latte::uuid(i)` – generates a random (type 4) UUID
- `latte::hash(i)` – generates a non-negative integer hash value
- `latte::hash2(a, b)` – generates a non-negative integer hash value of two integers
- `latte::hash_range(i, max)` – generates an integer value in range `0..max`
- `latte::blob(i, len)` – generates a random binary blob of length `len`

### Parameterizing workloads

Workloads can be parameterized by parameters given from the command line invocation.
Use `latte::param!(param_name, default_value)` macro to initialize script constants from command line parameters:

```rust
pub const LOAD_COUNT = latte::param!("row_count", 100);
```

Then you can set the parameter by using `-P`:

```
latte run <workload> -P row_count=200
```

Presently, only integer parameters are supported, but this limitation will be removed in the future.

### Error handling

Errors during execution of a workload script are divided into three classes:

- compile errors – the errors detected at the load time of the script; e.g. syntax errors or referencing an undefined
  variable. These are signalled immediately and terminate the benchmark even before connecting to the database.
- runtime errors / panics – e.g. division by zero or array out of bounds access. They terminate the benchmark
  immediately.
- error return values – e.g. when the query execution returns an error result. Those take effect only when actually
  returned from the function (use `?` for propagating them up the call chain). All errors except Cassandra overload
  errors terminate  
  the benchmark immediately. Overload errors (e.g. timeouts) that happen during the main run phase are counted and
  reported in the benchmark report.

## Example Report

```
./target/release/latte -w 1000  -n 10000000 read  -d 1 -s 2 localhost -x baseline.json
CONFIG =================================================================================================
                          ---------- This ---------- ---------- Other ----------    Change       
            Date        : Tue, 13 Oct 2020           Tue, 13 Oct 2020                              
            Time        : 10:13:33 +0200             10:12:50 +0200                                
           Label        :                                                                          
        Workload        : read                       read                                          
         Threads        :         1                           1                      +0.0%             
     Connections        :         1                           1                      +0.0%             
 Max parallelism   [req]:      1024                        1024                      +0.0%             
        Max rate [req/s]:                                                                          
          Warmup   [req]:      1000                        1000                      +0.0%             
      Iterations   [req]:   5000000                     5000000                      +0.0%             
        Sampling     [s]:       2.0                         2.0                      +0.0%             

LOG ====================================================================================================
    Time  Throughput        ----------------------- Response times [ms]---------------------------------
     [s]     [req/s]           Min        25        50        75        90        95        99       Max
   2.000      138650          0.56      5.49      6.88      8.49     10.38     11.90     15.52     28.11
   4.000      140494          1.03      5.53      6.81      8.34     10.19     11.61     14.42     24.40
   6.000      139572          1.92      5.59      6.82      8.37     10.18     11.49     14.99     28.90
   8.000      136651          0.95      5.78      7.00      8.49     10.30     11.66     14.69     27.15
  10.000      138886          0.55      5.61      6.87      8.49     10.28     11.53     14.66     30.32
  12.000      133352          2.00      5.72      7.01      8.83     10.97     12.59     17.44     26.93
  14.000      132942          0.77      5.70      7.05      8.88     10.97     12.48     17.20     34.78
  16.000      139092          0.79      5.70      6.93      8.40     10.10     11.32     14.08     22.46
  18.000      136143          0.77      5.78      7.08      8.53     10.14     11.46     15.19     38.24
  20.000      124534          2.31      5.88      7.44      9.37     11.90     13.57     18.29     50.81
  22.000      117656          0.70      6.09      7.83     10.13     12.95     15.92     22.17     38.21
  24.000      136094          2.69      5.78      7.09      8.64     10.30     11.52     14.19     23.02
  26.000      131638          2.11      5.83      7.33      9.05     10.79     11.96     15.57     27.33
  28.000      131253          0.80      5.63      7.11      8.94     11.30     13.17     18.46     42.01
  30.000      135951          1.09      5.57      7.03      8.80     10.75     12.05     15.35     27.65
  32.000      134118          1.40      5.84      7.07      8.73     10.62     11.94     15.49     24.13
  34.000      129356          1.14      5.86      7.39      9.22     11.19     12.65     17.38     29.33
  36.000      132904          1.56      5.59      7.16      8.91     10.90     12.52     15.55     25.81

SUMMARY STATS ==========================================================================================
                          ---------- This ---------- ---------- Other ----------    Change       P-value
         Elapsed     [s]:    37.395                      36.265                      +3.1%             
        CPU time     [s]:    54.574          ( 18.2%)    53.283          ( 18.4%)    +2.4%             
       Completed   [req]:   5000000          (100.0%)   5000000          (100.0%)    +0.0%             
          Errors   [req]:         0          (  0.0%)         0          (  0.0%)                    
      Partitions        :   5000000                     5000000                      +0.0%             
            Rows        :   5000000                     5000000                      +0.0%             
         Samples        :        18                          18                      +0.0%             
Mean sample size   [req]:    267698                      275767                      -2.9%             
     Parallelism   [req]:    1005.7          ( 98.2%)    1005.6          ( 98.2%)    +0.0%             
      Throughput [req/s]:    133849 ± 6084               137884 ± 2342               -2.9%  (   ) 0.0540
 Mean resp. time    [ms]:      7.54 ± 0.36                 7.31 ± 0.13               +3.1%  (   ) 0.0591

THROUGHPUT DISTRIBUTION [req/s] ========================================================================
                          ---------- This ---------- ---------- Other ----------    Change       
             Min        :    117656                      129696                      -9.3%             
               1        :    117656                      129696                      -9.3%             
               2        :    117656                      129696                      -9.3%             
               5        :    117656                      129696                      -9.3%             
              10        :    124534                      133613                      -6.8%             
              25        :    131638                      137624                      -4.3%             
              50        :    134118                      138236                      -3.0%             
              75        :    138650                      139909                      -0.9%             
              90        :    139571                      141186                      -1.1%             
              95        :    140493                      141187                      -0.5%             
             Max        :    140493                      141187                      -0.5%             

MEAN RESPONSE TIMES [ms] ===============================================================================
                          ---------- This ---------- ---------- Other ----------    Change       P-value
             Min        :      1.29 ± 0.51                 1.30 ± 0.37               -1.1%  (   ) 0.9417
              25        :      5.72 ± 0.16                 5.62 ± 0.08               +1.8%  (   ) 0.0633
              50        :      7.11 ± 0.31                 6.93 ± 0.08               +2.5%  (   ) 0.0874
              75        :      8.81 ± 0.46                 8.53 ± 0.19               +3.3%  (   ) 0.0721
              90        :     10.79 ± 0.63                10.37 ± 0.44               +4.1%  (   ) 0.0766
              95        :     12.30 ± 0.85                11.70 ± 0.54               +5.1%  (   ) 0.0615
              98        :     14.35 ± 1.21                13.55 ± 0.66               +5.9%  (   ) 0.0669
              99        :     16.15 ± 1.58                15.19 ± 0.69               +6.3%  (   ) 0.0802
            99.9        :     23.97 ± 6.46                23.52 ± 4.16               +1.9%  (   ) 0.8504
           99.99        :     28.67 ± 5.81                26.90 ± 4.77               +6.6%  (   ) 0.4436
             Max        :     30.53 ± 5.81                27.97 ± 4.78               +9.2%  (   ) 0.2702

RESPONSE TIME DISTRIBUTION =============================================================================
Percentile    Resp. time [ms]  ------------------------------- Count -----------------------------------
   0.00000          0.55               0   
   0.00024          0.69              12   
   0.00112          0.86              44   
   0.00416          1.07             152   
   0.01230          1.34             407   
   0.02950          1.68             860   
   0.07004          2.10            2027   
   0.17812          2.63            5404   
   0.55220          3.28           18704   
   3.43502          4.10          144141   **
  15.82948          5.13          619723   ************
  37.44670          6.41         1080861   *********************
  65.23694          8.02         1389512   ***************************
  85.77344         10.02         1026825   ********************
  95.53694         12.53          488175   *********
  98.67798         15.66          157052   ***
  99.61314         19.57           46758   
  99.89832         24.47           14259   
  99.96736         30.59            3452   
  99.97860         38.23             562   
  99.99660         47.79             900   
 100.00000         59.74             170   
```

