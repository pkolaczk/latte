# Lightweight Benchmarking Tool for Apache Cassandra

**Runs custom CQL workloads against a Cassandra cluster and measures throughput and response times**

![animation](img/latte.gif)

## Why Yet Another Benchmarking Program?

- Latte outperforms other benchmarking tools for Apache Cassandra by a wide margin. See [benchmarks](BENCHMARKS.md).
- Latte aims to offer the most flexible way of defining workloads.

### Performance

Contrary to 
[NoSQLBench](https://github.com/nosqlbench/nosqlbench), 
[Cassandra Stress](https://cassandra.apache.org/doc/4.0/cassandra/tools/cassandra_stress.html)
and [tlp-stress](https://thelastpickle.com/tlp-stress/), 
Latte has been written in Rust and uses the native Cassandra driver from Scylla. 
It features a fully asynchronous, thread-per-core execution engine, 
capable of running thousands of requests per second from a single thread. 

Latte has the following unique performance characteristics:
* Great scalability on multi-core machines.
* About 10x better CPU efficiency than NoSQLBench. 
  This means you can test large clusters with a small number of clients.  
* About 50x-100x lower memory footprint than Java-based tools. 
* Very low impact on operating system resources – low number of syscalls, context switches and page faults.  
* No client code warmup needed. The client code works with maximum performance from the first benchmark cycle. 
  Even runs as short as 30 seconds give accurate results.  
* No GC pauses nor HotSpot recompilation happening in the middle of the test. You want to measure hiccups of the server,
  not the benchmarking tool.

The excellent performance makes it a perfect tool for exploratory benchmarking, when you quickly want to experiment with
different workloads.

### Flexibility

Other benchmarking tools often use configuration files to specify workload recipes. 
Although that makes it easy to define simple workloads, it quickly becomes cumbersome when you want 
to script more realistic scenarios that issue multiple
queries or need to generate data in different ways than the ones directly built into the tool.

Instead of trying to bend a popular configuration file format into a turing-complete scripting language, Latte simply
embeds a real, fully-featured, turing-complete, modern scripting language. We chose [Rune](https://rune-rs.github.io/)
due to painless integration with Rust, first-class async support, satisfying performance and great support from its
maintainers.

Rune offers syntax and features similar to Rust, albeit with dynamic typing and easy automatic memory management. Hence,
you can not only just issue custom CQL queries, but you can program  
anything you wish. There are variables, conditional statements, loops, pattern matching, functions, lambdas,
user-defined data structures, objects, enums, constants, macros and many more.

## Features
* Compatible with Apache Cassandra 3.x, 4.x, DataStax Enterprise 6.x and ScyllaDB 
* Custom workloads with a powerful scripting engine
* Asynchronous queries
* Prepared queries
* Programmable data generation
* Workload parameterization
* Accurate measurement of throughput and response times with error margins
* No coordinated omission
* Configurable number of connections and threads
* Rate and concurrency limiters
* Progress bars
* Beautiful text reports
* Can dump report in JSON
* Side-by-side comparison of two runs
* Statistical significance analysis of differences corrected for auto-correlation

## Limitations

Latte is still early stage software under intensive development.

* Binding some CQL data types is not yet supported, e.g. user defined types, maps or integer types smaller than 64-bit.
* Query result sets are not exposed yet.
* The set of data generating functions is tiny and will be extended soon.
* Backwards compatibility may be broken frequently.

## Installation
1. [Install Rust toolchain](https://rustup.rs/)
2. Run `cargo install --git https://github.com/pkolaczk/latte`

## Usage

Start a Cassandra cluster somewhere (can be a local node). Then run:

```shell
latte run <workload.rn> [<node address>] 
 ```

You can find a few example workload files in the `workloads` folder.

Latte produces text reports on stdout but also saves all data to a json file in the working directory. The name of the
file is created automatically from the parameters of the run and a timestamp.

You can display the results of a previous run with `latte show`:

```shell
latte show <report.json>
latte show <report.json> -b <previous report.json>  # to compare against baseline performance
```

Run `latte --help` to display help with the available options.

## Workloads

Workloads for Latte are fully customizable with embedded scripting language [Rune](https://rune-rs.github.io/).

A workload script defines a set of public functions that Latte calls automatically. A minimum viable workload script
must define at least a single public async function `run` with two arguments:

- `ctx` – session context that provides the access to Cassandra
- `i` – current unique cycle number of a 64-bit integer type, starting at 0

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
functions accept an integer `i` cycle number, so you can generate consistent numbers. The data generating functions
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
