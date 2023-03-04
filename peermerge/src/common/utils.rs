pub(crate) struct YieldNow(pub(crate) bool);

impl core::future::Future for YieldNow {
    type Output = ();
    fn poll(
        mut self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<()> {
        if self.0 {
            core::task::Poll::Ready(())
        } else {
            self.set(Self(true));
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }
    }
}

// The futures::lock::Mutex is significantly slower than tokio/async-std Mutexes
// so use that only for WASM builds.
#[cfg(all(not(target_arch = "wasm32"), feature = "async-std"))]
pub(crate) type Mutex<T> = async_std::sync::Mutex<T>;
#[cfg(all(not(target_arch = "wasm32"), feature = "tokio"))]
pub(crate) type Mutex<T> = tokio::sync::Mutex<T>;
#[cfg(target_arch = "wasm32")]
pub(crate) type Mutex<T> = futures::lock::Mutex<T>;
