// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

mod callback;
mod executor;
mod lock;
mod promise;

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use self::callback::{Abort, Request as RequestCallback, UnaryRequest as UnaryRequestCallback};
use self::executor::SpawnTask;
use self::promise::{Batch as BatchPromise, Shutdown as ShutdownPromise};
use crate::call::server::RequestContext;
use crate::call::{BatchContext, Call, MessageReader};
use crate::cq::CompletionQueue;
use crate::server::RequestCallContext;

pub(crate) use self::executor::{Executor, Kicker, UnfinishedWork};
pub(crate) use self::lock::check_alive;
pub use self::lock::{PairFuture as CqFuture, SpinLock};
pub use self::promise::BatchType;

/// Future object for batch jobs.
pub type BatchFuture = CqFuture<Option<MessageReader>>;

/// A result holder for asynchronous execution.
// This enum is going to be passed to FFI, so don't use trait or generic here.
pub enum CallTag {
    Batch(BatchPromise),
    Request(RequestCallback),
    UnaryRequest(UnaryRequestCallback),
    Abort(Abort),
    Shutdown(ShutdownPromise),
    Spawn(Arc<SpawnTask>),
}

impl CallTag {
    /// Generate a Future/CallTag pair for batch jobs.
    pub fn batch_pair(ty: BatchType) -> (BatchFuture, CallTag) {
        let (f, h) = self::lock::pair();
        let batch = BatchPromise::new(ty, h);
        (f, CallTag::Batch(batch))
    }

    /// Generate a CallTag for request job. We don't have an eventloop
    /// to pull the future, so just the tag is enough.
    pub fn request(ctx: RequestCallContext) -> CallTag {
        CallTag::Request(RequestCallback::new(ctx))
    }

    /// Generate a Future/CallTag pair for shutdown call.
    pub fn shutdown_pair() -> (CqFuture<()>, CallTag) {
        let (f, h) = self::lock::pair();
        let shutdown = ShutdownPromise::new(h);
        (f, CallTag::Shutdown(shutdown))
    }

    /// Generate a CallTag for abort call before handler is called.
    pub fn abort(call: Call) -> CallTag {
        CallTag::Abort(Abort::new(call))
    }

    /// Generate a CallTag for unary request job.
    pub fn unary_request(ctx: RequestContext, rc: RequestCallContext) -> CallTag {
        let cb = UnaryRequestCallback::new(ctx, rc);
        CallTag::UnaryRequest(cb)
    }

    /// Get the batch context from result holder.
    pub fn batch_ctx(&self) -> Option<&BatchContext> {
        match *self {
            CallTag::Batch(ref prom) => Some(prom.context()),
            CallTag::UnaryRequest(ref cb) => Some(cb.batch_ctx()),
            CallTag::Abort(ref cb) => Some(cb.batch_ctx()),
            _ => None,
        }
    }

    /// Get the request context from the result holder.
    pub fn request_ctx(&self) -> Option<&RequestContext> {
        match *self {
            CallTag::Request(ref prom) => Some(prom.context()),
            CallTag::UnaryRequest(ref cb) => Some(cb.request_ctx()),
            _ => None,
        }
    }

    /// Resolve the CallTag with given status.
    pub fn resolve(self, cq: &CompletionQueue, success: bool) {
        match self {
            CallTag::Batch(prom) => prom.resolve(success),
            CallTag::Request(cb) => cb.resolve(cq, success),
            CallTag::UnaryRequest(cb) => cb.resolve(cq, success),
            CallTag::Abort(_) => {}
            CallTag::Shutdown(prom) => prom.resolve(success),
            CallTag::Spawn(notify) => self::executor::resolve(cq, notify, success),
        }
    }
}

impl Debug for CallTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            CallTag::Batch(ref ctx) => write!(f, "CallTag::Batch({:?})", ctx),
            CallTag::Request(_) => write!(f, "CallTag::Request(..)"),
            CallTag::UnaryRequest(_) => write!(f, "CallTag::UnaryRequest(..)"),
            CallTag::Abort(_) => write!(f, "CallTag::Abort(..)"),
            CallTag::Shutdown(_) => write!(f, "CallTag::Shutdown"),
            CallTag::Spawn(_) => write!(f, "CallTag::Spawn"),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use std::sync::mpsc::*;
    use std::sync::*;
    use std::thread;

    use super::*;
    use crate::env::Environment;
    use crate::error::Error;

    #[test]
    fn test_resolve() {
        let env = Environment::new(1);

        let (cq_f1, tag1) = CallTag::shutdown_pair();
        let (cq_f2, tag2) = CallTag::shutdown_pair();
        let (tx, rx) = mpsc::channel();

        let handler = thread::spawn(move || {
            tx.send(cq_f1.wait()).unwrap();
            tx.send(cq_f2.wait()).unwrap();
        });

        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        tag1.resolve(&env.pick_cq(), true);
        assert!(rx.recv().unwrap().is_ok());

        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        tag2.resolve(&env.pick_cq(), false);
        match rx.recv() {
            Ok(Err(Error::ShutdownFailed)) => {}
            res => panic!("expect shutdown failed, but got {:?}", res),
        }

        handler.join().unwrap();
    }
}
