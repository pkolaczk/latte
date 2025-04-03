use crate::adapters::scylla::ScyllaAdapter;
use crate::adapters::Adapters;
use crate::config::ConnectionConf;
use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::context::Context;
use crate::scripting::executor::Executor;
use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::policies::load_balancing::DefaultPolicy;

fn ssl_context(conf: &&ConnectionConf) -> Result<Option<SslContext>, CassError> {
    if conf.ssl {
        let mut ssl = SslContextBuilder::new(SslMethod::tls())?;
        if let Some(path) = &conf.ssl_ca_cert_file {
            ssl.set_ca_file(path)?;
        }
        if let Some(path) = &conf.ssl_cert_file {
            ssl.set_certificate_file(path, SslFiletype::PEM)?;
        }
        if let Some(path) = &conf.ssl_key_file {
            ssl.set_private_key_file(path, SslFiletype::PEM)?;
        }
        Ok(Some(ssl.build()))
    } else {
        Ok(None)
    }
}

/// Configures connection to Cassandra.
pub async fn connect(conf: &ConnectionConf) -> Result<Context, CassError> {
    let mut policy_builder = DefaultPolicy::builder().token_aware(true);
    if let Some(dc) = &conf.datacenter {
        policy_builder = policy_builder
            .prefer_datacenter(dc.to_owned())
            .permit_dc_failover(true);
    }
    let profile = ExecutionProfile::builder()
        .consistency(conf.consistency.scylla_consistency())
        .load_balancing_policy(policy_builder.build())
        .request_timeout(Some(conf.request_timeout))
        .build();

    let ssl_ctx = ssl_context(&conf)?;

    let scylla_session = SessionBuilder::new()
        .known_nodes(&conf.addresses)
        .pool_size(PoolSize::PerShard(conf.count))
        .user(&conf.user, &conf.password)
        .default_execution_profile_handle(profile.into_handle())
        // TODO: find out why it works in doc, but does not compile in real world
        //.tls_context(ssl_ctx)
        .build()
        .await
        .map_err(|e| CassError(CassErrorKind::FailedToConnect(conf.addresses.clone(), e)))?;
    Ok(Context::new(Adapters::Scylla(ScyllaAdapter::new(
        scylla_session,
        Executor::new(conf.retry_strategy, |_| true),
    ))))
}

pub struct ClusterInfo {
    pub name: String,
    pub cassandra_version: String,
}
