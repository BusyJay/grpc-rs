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

use std::fmt::{self, Debug, Formatter};

use super::lock::PairHolder;
use crate::call::{BatchContext, MessageReader, RpcStatusCode};
use crate::error::Error;

/// Batch job type.
#[derive(PartialEq, Debug)]
pub enum BatchType {
    /// Finish without reading any message.
    Finish,
    /// Extract one message when finish.
    Read,
    /// Check the rpc code and then extract one message.
    CheckRead,
}

/// A promise used to resolve batch jobs.
pub struct Batch {
    ty: BatchType,
    ctx: BatchContext,
    holder: PairHolder<Option<MessageReader>>,
}

impl Batch {
    pub fn new(ty: BatchType, holder: PairHolder<Option<MessageReader>>) -> Batch {
        Batch {
            ty,
            ctx: BatchContext::new(),
            holder,
        }
    }

    pub fn context(&self) -> &BatchContext {
        &self.ctx
    }

    fn read_one_msg(&mut self, success: bool) {
        if success {
            self.holder.set_result(Ok(self.ctx.recv_message()))
        } else {
            // rely on C core to handle the failed read (e.g. deliver approriate
            // statusCode on the clientside).
            self.holder.set_result(Ok(None))
        }
    }

    fn finish_response(&mut self, succeed: bool) {
        if succeed {
            let status = self.ctx.rpc_status();
            if status.status == RpcStatusCode::OK {
                self.holder.set_result(Ok(None))
            } else {
                self.holder.set_result(Err(Error::RpcFailure(status)))
            }
        } else {
            self.holder.set_result(Err(Error::RemoteStopped))
        }
    }

    fn handle_unary_response(&mut self) {
        let status = self.ctx.rpc_status();
        if status.status == RpcStatusCode::OK {
            self.holder.set_result(Ok(self.ctx.recv_message()))
        } else {
            self.holder.set_result(Err(Error::RpcFailure(status)))
        }
    }

    pub fn resolve(mut self, success: bool) {
        match self.ty {
            BatchType::CheckRead => {
                assert!(success);
                self.handle_unary_response();
            }
            BatchType::Finish => {
                self.finish_response(success);
            }
            BatchType::Read => {
                self.read_one_msg(success);
            }
        }
    }
}

impl Debug for Batch {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Batch [{:?}]", self.ty)
    }
}

/// A promise used to resolve async shutdown result.
pub struct Shutdown {
    holder: PairHolder<()>,
}

impl Shutdown {
    pub fn new(holder: PairHolder<()>) -> Shutdown {
        Shutdown { holder }
    }

    pub fn resolve(mut self, success: bool) {
        if success {
            self.holder.set_result(Ok(()))
        } else {
            self.holder.set_result(Err(Error::ShutdownFailed))
        }
    }
}
