use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use fastwebsockets::*;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::OwnedTrustAnchor;
use tokio_rustls::TlsConnector;

fn tls_connector() -> Result<TlsConnector> {
  let mut root_store = tokio_rustls::rustls::RootCertStore::empty();

  root_store.add_server_trust_anchors(
    webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
      OwnedTrustAnchor::from_subject_spki_name_constraints(
        ta.subject,
        ta.spki,
        ta.name_constraints,
      )
    }),
  );

  let config = ClientConfig::builder()
    .with_safe_defaults()
    .with_root_certificates(root_store)
    .with_no_client_auth();

  Ok(TlsConnector::from(Arc::new(config)))
}

pub fn foo() {
    let uri = "wss://localhost:8080";
    let client = ClientBuilder::new(uri).unwrap().add_protocol("");
    println!("bar")
}
