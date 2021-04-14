// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use grpcio_sys::{grpc_op, grpc_op_type, grpc_byte_buffer, grpc_compression_level, grpc_call_error, grpc_call};
use std::mem::MaybeUninit;
use std::{ptr, slice};
use std::ffi::c_void;
use crate::{Metadata, MetadataBuilder, WriteFlags, RpcStatus, RpcStatusCode};
use crate::buf::{GrpcByteBuffer, GrpcSlice};

const MAX_OPS: usize = 6;

pub(crate) unsafe trait Op {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool;
    fn finish_op(&mut self, status: bool);
}

#[derive(Default)]
pub struct NoOp;

unsafe impl Op for NoOp {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool { false }
    fn finish_op(&mut self, status: bool) {}
}

pub struct SendInitialMetadata {
    metadata: Option<Metadata>,
    flags: u32,
    compression_level: Option<grpc_compression_level>
}

impl SendInitialMetadata {
    pub fn new(meta: Option<Metadata>, flags: u32) -> SendInitialMetadata {
        SendInitialMetadata {
            metadata: meta,
            flags,
            compression_level: None,
        }
    }

    pub fn set_compression_level(&mut self, level: grpc_compression_level) {
        self.compression_level = Some(level);
    }
}

unsafe impl Op for SendInitialMetadata {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool {
        if self.metadata.is_none() && self.compression_level.is_none() {
            return false;
        }
        let ptr = op.as_mut_ptr();
        unsafe {
            (*ptr).op = grpc_op_type::GRPC_OP_SEND_INITIAL_METADATA;
            (*ptr).reserved = ptr::null_mut();
            (*ptr).flags = self.flags;
            if let Some(metadata) = &mut self.metadata {
                (*ptr).data.send_initial_metadata.count = metadata.len();
                (*ptr).data.send_initial_metadata.metadata = metadata.as_mut_ptr();
            } else {
                (*ptr).data.send_initial_metadata.count = 0;
                (*ptr).data.send_initial_metadata.metadata = ptr::null_mut();
            }
            if let Some(level) = self.compression_level {
                (*ptr).data.send_initial_metadata.maybe_compression_level.level = level;
                (*ptr).data.send_initial_metadata.maybe_compression_level.is_set = 1;
            } else {
                (*ptr).data.send_initial_metadata.maybe_compression_level.is_set = 0;
            }
        }
        true
    }

    fn finish_op(&mut self, _status: bool) {
        self.metadata = None;
        self.compression_level = None;
    }
}

#[derive(Default)]
pub struct SendMessage {
    buf: Option<GrpcByteBuffer>,
    flags: WriteFlags,
}

impl SendMessage {
    pub fn new(buf: GrpcByteBuffer, flags: WriteFlags) -> SendMessage {
        SendMessage {
            buf: Some(buf),
            flags,
        }
    }

    pub fn send_message(&mut self, buf: GrpcByteBuffer, flags: WriteFlags) {
        self.buf = Some(buf);
        self.flags = flags;
    }

    pub fn has_pending_bytes(&self) -> bool {
        self.buf.is_some()
    }
}

unsafe impl Op for SendMessage {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool {
        let buf = match &mut self.buf {
            Some(b) => b.as_mut_ptr(),
            None => return false,
        };
        let ptr = op.as_mut_ptr();
        unsafe {
            (*ptr).op = grpc_op_type::GRPC_OP_SEND_MESSAGE;
            (*ptr).reserved = ptr::null_mut();
            (*ptr).flags = self.flags.flags();
            (*ptr).data.send_message.send_message = buf;
        }
        true
    }

    fn finish_op(&mut self, _status: bool) {
        self.buf = None;
    }
}

#[derive(Default)]
pub struct RecvMessage {
    recv_buf: Option<*mut grpc_byte_buffer>,
}

impl RecvMessage {
    pub unsafe fn take_message(&mut self) -> Option<GrpcByteBuffer> {
        self.recv_buf.take().map(|p| GrpcByteBuffer::from_raw(p))
    }

    pub fn recv_message(&mut self) {
        self.recv_buf = Some(ptr::null_mut());
    }
}

unsafe impl Op for RecvMessage {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool {
        let buf = match &mut self.recv_buf {
            None => return false,
            Some(buf) => buf,
        };
        let ptr = op.as_mut_ptr();
        unsafe {
            (*ptr).op = grpc_op_type::GRPC_OP_RECV_MESSAGE;
            (*ptr).reserved = ptr::null_mut();
            (*ptr).flags = 0;
            (*ptr).data.recv_message.recv_message = buf;
        }
        true
    }

    fn finish_op(&mut self, _status: bool) {}
}

#[derive(Default)]
pub struct ClientSendClose {
    closed: bool,
}

impl ClientSendClose {
    pub fn mark_closed(&mut self) {
        self.closed = true;
    }

    pub fn closed(&self) -> bool {
        self.closed
    }
}

unsafe impl Op for ClientSendClose {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool {
        if !self.closed {
            return false;
        }

        let ptr = op.as_mut_ptr();
        unsafe {
            (*ptr).op = grpc_op_type::GRPC_OP_SEND_CLOSE_FROM_CLIENT;
            (*ptr).reserved = ptr::null_mut();
            (*ptr).flags = 0;
        }
        true
    }

    fn finish_op(&mut self, _status: bool) {}
}

#[derive(Default)]
pub struct ServerSendStatus {
    status: Option<(RpcStatus, Option<Metadata>)>,
    slice: GrpcSlice,
}

unsafe impl Op for ServerSendStatus {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool {
        let (status, metadata) = match &mut self.status {
            None => return false,
            Some(res) => res,
        };
        
        let ptr = op.as_mut_ptr();
        unsafe {
            (*ptr).op = grpc_op_type::GRPC_OP_SEND_STATUS_FROM_SERVER;
            (*ptr).reserved = ptr::null_mut();
            (*ptr).flags = 0;
            if let Some(metadata) = metadata {
                (*ptr).data.send_status_from_server.trailing_metadata_count = metadata.len();
                (*ptr).data.send_status_from_server.trailing_metadata = metadata.as_mut_ptr();
            } else {
                (*ptr).data.send_status_from_server.trailing_metadata_count = 0;
                (*ptr).data.send_status_from_server.trailing_metadata = ptr::null_mut();
            }
            (*ptr).data.send_status_from_server.status = status.code().into();
            if status.message().is_empty() {
                (*ptr).data.send_status_from_server.status_details = ptr::null_mut();
            } else {
                // hack: it's not a static slice at all. But grpc c core only requires it
                // lives until calling finish_op, which is guaranteed by this struct.
                // Calling `from_static_slice` to avoid allocation and copy.
                let msg_ptr = status.message().as_ptr();
                let msg_len = status.message().len();
                self.slice = GrpcSlice::from_static_slice(slice::from_raw_parts(msg_ptr, msg_len));
                (*ptr).data.send_status_from_server.status_details = self.slice.as_mut_ptr();
            }
        }
        true
    }

    fn finish_op(&mut self, _status: bool) {
        self.status = None;
    }
}

pub struct RecvInitialMetadata {
    metadata: Option<Metadata>,
}

impl RecvInitialMetadata {
    pub fn take_metadata(&mut self) -> Option<Metadata> {
        self.metadata.take()
    }
}

impl Default for RecvInitialMetadata {
    fn default() -> RecvInitialMetadata {
        RecvInitialMetadata {
            metadata: Some(MetadataBuilder::new().build()),
        }
    }
}

unsafe impl Op for RecvInitialMetadata {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool {
        let ptr = op.as_mut_ptr();
        unsafe {
            (*ptr).op = grpc_op_type::GRPC_OP_RECV_INITIAL_METADATA;
            (*ptr).reserved = ptr::null_mut();
            (*ptr).flags = 0;
            (*ptr).data.recv_initial_metadata.recv_initial_metadata = self.metadata.as_mut().unwrap() as *mut _ as _;
        }
        true
    }

    fn finish_op(&mut self, _status: bool) {}
}

pub struct ClientRecvStatus {
    metadata: Option<Metadata>,
    details: GrpcSlice,
    status: Option<RpcStatus>,
}

impl ClientRecvStatus {
    pub fn take_status(&mut self) -> Option<RpcStatus> {
        self.status.take()
    }

    pub fn take_metadata(&mut self) -> Option<Metadata> {
        self.metadata.take()
    }
}

impl Default for ClientRecvStatus {
    fn default() -> ClientRecvStatus {
        ClientRecvStatus {
            metadata: Some(MetadataBuilder::new().build()),
            details: GrpcSlice::default(),
            status: Some(RpcStatus::ok()),
        }
    }
}

unsafe impl Op for ClientRecvStatus {
    fn fill_op(&mut self, op: &mut MaybeUninit<grpc_op>) -> bool {
        let ptr = op.as_mut_ptr();
        unsafe {
            (*ptr).op = grpc_op_type::GRPC_OP_RECV_STATUS_ON_CLIENT;
            (*ptr).reserved = ptr::null_mut();
            (*ptr).flags = 0;
            (*ptr).data.recv_status_on_client.trailing_metadata = &mut self.metadata as *mut _ as _;
            (*ptr).data.recv_status_on_client.status = &mut self.status.as_mut().unwrap().code as *mut _ as _;
            (*ptr).data.recv_status_on_client.status_details = self.details.as_mut_ptr();
            (*ptr).data.recv_status_on_client.error_string = ptr::null_mut();
        }
        true
    }

    fn finish_op(&mut self, status: bool) {
        assert!(status);
        let code = self.status.as_ref().unwrap().code();
        if code == RpcStatusCode::OK {
            return;
        }
        let message = String::from_utf8_lossy(self.details.as_slice()).into_owned();
        let details = if let Some(m) = &self.metadata {
            m.search_binary_error_details().to_vec()
        } else {
            vec![]
        };
        self.status = Some(RpcStatus::with_details(code, message, details));
    }
}

pub(crate) struct CallOpSet<Op0 = NoOp, Op1 = NoOp, Op2 = NoOp, Op3 = NoOp, Op4 = NoOp, Op5 = NoOp> {
    pub op0: Op0,
    pub op1: Op1,
    pub op2: Op2,
    pub op3: Op3,
    pub op4: Op4,
    pub op5: Op5,
    pub resolved: bool,
}

impl<Op0, Op1, Op2, Op3, Op4, Op5> CallOpSet<Op0, Op1, Op2, Op3, Op4, Op5>
where
    Op0: Op,
    Op1: Op,
    Op2: Op,
    Op3: Op,
    Op4: Op,
    Op5: Op,
{
    pub fn call(&mut self, call: *mut grpc_call, tag: *mut c_void) {
        unsafe {
            let mut ops: [MaybeUninit<grpc_op>; MAX_OPS] = MaybeUninit::uninit().assume_init();
            let mut idx = 0;
            idx += self.op0.fill_op(&mut ops[idx]) as usize;
            idx += self.op1.fill_op(&mut ops[idx]) as usize;
            idx += self.op2.fill_op(&mut ops[idx]) as usize;
            idx += self.op3.fill_op(&mut ops[idx]) as usize;
            idx += self.op4.fill_op(&mut ops[idx]) as usize;
            idx += self.op5.fill_op(&mut ops[idx]) as usize;
            let p = ops.as_mut_ptr() as *mut grpc_op;
            let error = grpcio_sys::grpc_call_start_batch(call, p, idx, tag, ptr::null_mut());
            if error != grpc_call_error::GRPC_CALL_OK {
                // A failure here indicates an API misuse
                panic!("unexpected error code: {:?}", error);
            }
        }
    }

    pub fn finalize_result(&mut self, status: bool) {
        self.op0.finish_op(status);
        self.op1.finish_op(status);
        self.op2.finish_op(status);
        self.op3.finish_op(status);
        self.op4.finish_op(status);
        self.op5.finish_op(status);
        self.resolved = true;
    }
}
