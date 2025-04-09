use crate::adapters::aerospike::AerospikeAdapter;
use crate::adapters::postgres::PostgresAdapter;
use crate::adapters::scylla::ScyllaAdapter;
use crate::adapters::Adapters;
use crate::config::{ConnectionConf, DBEngine};
use crate::scripting::cass_error::CassErrorKind::InitFailure;
use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::context::Context;
use crate::scripting::executor::Executor;
use aerospike::policy::BasePolicy;
use aerospike::{
    Client, ClientPolicy, Error, Expiration, GenerationPolicy, ReadPolicy, ResultCode, WritePolicy,
};
use anyhow::anyhow;
use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::errors::ExecutionError;
use scylla::policies::load_balancing::DefaultPolicy;
use tokio_postgres::{Config, NoTls};

#[allow(unused)]
fn ssl_context(conf: &&ConnectionConf) -> Result<Option<SslContext>, CassError> {
    if conf.scylla_connection_conf.ssl {
        let mut ssl = SslContextBuilder::new(SslMethod::tls())?;
        if let Some(path) = &conf.scylla_connection_conf.ssl_ca_cert_file {
            ssl.set_ca_file(path)?;
        }
        if let Some(path) = &conf.scylla_connection_conf.ssl_cert_file {
            ssl.set_certificate_file(path, SslFiletype::PEM)?;
        }
        if let Some(path) = &conf.scylla_connection_conf.ssl_key_file {
            ssl.set_private_key_file(path, SslFiletype::PEM)?;
        }
        Ok(Some(ssl.build()))
    } else {
        Ok(None)
    }
}

async fn connect_aerospike(conf: &ConnectionConf) -> Result<Context, CassError> {
    let mut policy = ClientPolicy::default();
    if !conf.user.is_empty() {
        policy
            .set_user_password(conf.user.clone(), conf.password.clone())
            .map_err(|e| CassError(InitFailure(anyhow!(e))))?;
    }
    policy.conn_pools_per_node = conf.count.get();

    let client = Client::new(&ClientPolicy::default(), &conf.addresses.join(","))
        .await
        .map_err(|e| CassError(InitFailure(anyhow!(e))))?;

    Ok(Context::new(Adapters::Aerospike(AerospikeAdapter::new(
        client,
        Executor::new(conf.retry_strategy, |res| match res {
            Ok(_) => false,
            Err(e) => match e {
                Error::ServerError(ResultCode::KeyNotFoundError, _, _) => false,
                _ => true,
            },
        }),
        ReadPolicy {
            priority: conf
                .aerospike_connection_conf
                .read_priority
                .to_sdk_priority(),
            consistency_level: conf
                .aerospike_connection_conf
                .consistency_level
                .to_sdk_consistency_level(),
            timeout: Some(conf.request_timeout),
            max_retries: Some(conf.aerospike_connection_conf.max_retries),
            sleep_between_retries: Some(conf.aerospike_connection_conf.sleep_between_retries),
            filter_expression: None,
        },
        WritePolicy {
            base_policy: BasePolicy {
                priority: conf
                    .aerospike_connection_conf
                    .write_priority
                    .to_sdk_priority(),
                consistency_level: conf
                    .aerospike_connection_conf
                    .consistency_level
                    .to_sdk_consistency_level(),
                timeout: Some(conf.request_timeout),
                max_retries: Some(conf.aerospike_connection_conf.max_retries),
                sleep_between_retries: Some(conf.aerospike_connection_conf.sleep_between_retries),
                filter_expression: None,
            },
            record_exists_action: conf
                .aerospike_connection_conf
                .record_exists_action
                .to_sdk_record_exists_action(),
            generation_policy: GenerationPolicy::None,
            commit_level: conf
                .aerospike_connection_conf
                .commit_level
                .to_sdk_commit_level(),
            generation: 0,
            expiration: Expiration::Seconds(conf.aerospike_connection_conf.expiration_seconds),
            send_key: false,
            respond_per_each_op: false,
            durable_delete: false,
            filter_expression: None,
        },
        conf.aerospike_connection_conf.namespace.clone(),
        conf.aerospike_connection_conf.set.clone(),
    ))))
}

async fn connect_scylla(conf: &ConnectionConf) -> Result<Context, CassError> {
    let mut policy_builder = DefaultPolicy::builder().token_aware(true);
    if let Some(dc) = &conf.scylla_connection_conf.datacenter {
        policy_builder = policy_builder
            .prefer_datacenter(dc.to_owned())
            .permit_dc_failover(true);
    }
    let profile = ExecutionProfile::builder()
        .consistency(conf.scylla_connection_conf.consistency.scylla_consistency())
        .load_balancing_policy(policy_builder.build())
        .request_timeout(Some(conf.request_timeout))
        .build();

    let scylla_session = SessionBuilder::new()
        .known_nodes(&conf.addresses)
        .pool_size(PoolSize::PerShard(conf.count))
        .user(&conf.user, &conf.password)
        .default_execution_profile_handle(profile.into_handle())
        // TODO: find out why it works in doc, but does not compile in real world
        //.tls_context(ssl_context(&conf))
        .build()
        .await
        .map_err(|e| CassError(CassErrorKind::FailedToConnect(conf.addresses.clone(), e)))?;
    Ok(Context::new(Adapters::Scylla(ScyllaAdapter::new(
        scylla_session,
        Executor::new(conf.retry_strategy, |res| match res {
            Err(ExecutionError::RequestTimeout(_)) => true,
            _ => false,
        }),
    ))))
}

async fn connect_postgres(conf: &ConnectionConf) -> Result<Context, CassError> {
    let mut config = Config::new();
    let mut config = config
        .user(conf.user.clone())
        .password(conf.password.clone())
        .dbname(conf.postgres_connection_conf.dbname.clone())
        .ssl_mode(conf.postgres_connection_conf.ssl_mode.to_postgres_enum())
        .ssl_negotiation(
            conf.postgres_connection_conf
                .ssl_negotiation
                .to_postgres_enum(),
        )
        .channel_binding(
            conf.postgres_connection_conf
                .channel_binding
                .to_postgres_enum(),
        )
        .load_balance_hosts(
            conf.postgres_connection_conf
                .load_balance_hosts
                .to_postgres_enum(),
        )
        .port(conf.postgres_connection_conf.port);

    for host in conf.addresses.clone() {
        config = config.host(&host);
    }

    let (client, connection) = config
        // TODO: support for TLS requires extra libs i.e. postgres-openssl or postgres-native-tls
        .connect(NoTls)
        .await
        .map_err(|e| CassError(CassErrorKind::PgPrepare("Connecting to DB".to_string(), e)))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(Context::new(Adapters::Postgres(PostgresAdapter::new(
        client,
        conf.request_timeout,
        Executor::new(conf.retry_strategy, |res| match res {
            Err(_) => true,
            _ => false,
        }),
    ))))
}

/// Configures connection to Cassandra.
pub async fn connect(conf: &ConnectionConf) -> Result<Context, CassError> {
    match conf.db {
        DBEngine::Scylla => connect_scylla(conf).await,
        DBEngine::Aerospike => connect_aerospike(conf).await,
        DBEngine::Foundation => Err(CassError(CassErrorKind::UnsupportedEngine)),
        DBEngine::PostgreSQL => connect_postgres(conf).await,
    }
}

pub struct ClusterInfo {
    pub name: String,
    pub cassandra_version: String,
}
