use libc::c_int;
use futures::{FutureExt, TryFutureExt};

pub const FOPEN_KEEP_CACHE: u32 = 1 << 1;

pub type FuseError = c_int;

pub fn wrap_read(
    executor: &tokio::runtime::TaskExecutor,
    reply: fuse::ReplyData,
    fut: impl std::future::Future<Output = Result<Vec<u8>, FuseError>> + Send + 'static,
) {
    executor.spawn(async {
        match fut.await {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(err),
        }
    }.unit_error().boxed().compat());
}
