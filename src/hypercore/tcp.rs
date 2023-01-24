use anyhow::Result;
use futures::stream::StreamExt;
use futures::Future;

#[cfg(feature = "async-std")]
use async_std::{
    net::{TcpListener, TcpStream},
    task,
};
#[cfg(feature = "tokio")]
use tokio::{
    net::{TcpListener, TcpStream},
    task,
};

#[cfg(feature = "async-std")]
pub async fn tcp_server<F, C>(
    port: u32,
    onconnection: impl Fn(TcpStream, bool, C) -> F + Send + Sync + Copy + 'static,
    context: C,
) -> Result<()>
where
    F: Future<Output = Result<()>> + Send,
    C: Clone + Send + 'static,
{
    let listener = TcpListener::bind(&format!("localhost:{}", port)).await?;
    let mut incoming = listener.incoming();
    while let Some(Ok(stream)) = incoming.next().await {
        let context = context.clone();
        let _peer_addr = stream.peer_addr().unwrap();
        task::spawn(async move {
            onconnection(stream, false, context)
                .await
                .expect("Should return ok");
        });
    }
    Ok(())
}

#[cfg(feature = "tokio")]
pub async fn tcp_server<F, C>(
    port: u32,
    onconnection: impl Fn(TcpStream, bool, C) -> F + Send + Sync + Copy + 'static,
    context: C,
) -> Result<()>
where
    F: Future<Output = Result<()>> + Send,
    C: Clone + Send + 'static,
{
    let listener = TcpListener::bind(&format!("localhost:{}", port)).await?;

    while let Ok((stream, _peer_address)) = listener.accept().await {
        let context = context.clone();
        task::spawn(async move {
            onconnection(stream, false, context)
                .await
                .expect("Should return ok");
        });
    }
    Ok(())
}

pub async fn tcp_client<F, C>(
    port: u32,
    onconnection: impl Fn(TcpStream, bool, C) -> F + Send + Sync + Copy + 'static,
    context: C,
) -> Result<()>
where
    F: Future<Output = Result<()>> + Send,
    C: Clone + Send + 'static,
{
    let stream = TcpStream::connect(&format!("localhost:{}", port)).await?;
    onconnection(stream, true, context).await
}
