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
