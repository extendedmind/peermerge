use anyhow::Result;
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use futures::stream::StreamExt;
use futures::Future;

pub async fn tcp_server<F, C>(
    url: &str,
    onconnection: impl Fn(TcpStream, bool, C) -> F + Send + Sync + Copy + 'static,
    context: C,
) -> Result<()>
where
    F: Future<Output = Result<()>> + Send,
    C: Clone + Send + 'static,
{
    let listener = TcpListener::bind(&url).await?;
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

pub async fn tcp_client<F, C>(
    url: &str,
    onconnection: impl Fn(TcpStream, bool, C) -> F + Send + Sync + Copy + 'static,
    context: C,
) -> Result<()>
where
    F: Future<Output = Result<()>> + Send,
    C: Clone + Send + 'static,
{
    let stream = TcpStream::connect(&url).await?;
    onconnection(stream, true, context).await
}
