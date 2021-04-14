// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::call::{Call, RpcStatus};
use crate::call_v2::ops::{CallOpSet, SendInitialMetadata, SendMessage, ClientSendClose, RecvInitialMetadata, RecvMessage, ClientRecvStatus};
use crate::call_v2::tag::{RefCallCell, Tag};
use crate::{Channel, Method, GrpcSlice, CallOption, Result, Metadata, RpcStatusCode, Error, WriteFlags};
use crate::buf::GrpcByteBufferReader;
use crate::codec::{DeserializeFn, SerializeFn};
use std::marker::PhantomData;
use crate::buf::GrpcByteBuffer;
use std::mem::MaybeUninit;
use futures::task::{Waker, Context, Poll};
use futures::prelude::*;
use std::ptr::{self, addr_of_mut};

struct UnaryReaderImpl {
    call: Call,
    tag: Tag<UnaryReaderImpl>,
    ops: CallOpSet<SendInitialMetadata, SendMessage, ClientSendClose, RecvInitialMetadata, RecvMessage, ClientRecvStatus>,
}

pub struct UnaryReader<Resp> {
    cell: RefCallCell<UnaryReaderImpl>,
    resp_de: DeserializeFn<Resp>,
}

pub fn unary<Req, Resp>(channel: &Channel, method: &Method<Req, Resp>, req: &Req, opt: CallOption) -> Result<UnaryReader<Resp>> {
    let mut call = channel.create_call(method, &opt)?;
    let cell = unsafe {
        RefCallCell::new_in(call.call, move |im: &mut MaybeUninit<UnaryReaderImpl>, ptr| {
            let mut payload = GrpcSlice::default();
            (method.req_ser())(req, &mut payload);

            let buf = GrpcByteBuffer::from(&payload);
            unsafe {
                let l = im.as_mut_ptr();
                addr_of_mut!((*l).tag).write(Tag::new(ptr));
                addr_of_mut!((*l).ops.op0).write(SendInitialMetadata::new(opt.headers, opt.call_flags));
                addr_of_mut!((*l).ops.op1).write(SendMessage::new(buf, opt.write_flags));
                addr_of_mut!((*l).ops.op2).write(Default::default());
                addr_of_mut!((*l).ops.op3).write(Default::default());
                addr_of_mut!((*l).ops.op4).write(Default::default());
                addr_of_mut!((*l).ops.op5).write(Default::default());
                addr_of_mut!((*l).ops.resolved).write(false);
                let p = (*l).tag.get_tag();
                (*l).ops.call(call.call, p);
                addr_of_mut!((*l).call).write(call);
            }
        })
    };
    

    Ok(UnaryReader {
        cell,
        resp_de: method.resp_de(),
    })
}

impl<Resp> UnaryReader<Resp> {
    async fn ensure_finished(&mut self) {
        if !self.cell.as_mut().ops.resolved {
            let b = (&mut self.cell.as_mut().tag).await;
            assert!(b);
            self.cell.as_mut().ops.finalize_result(b);
        }
    }

    pub async fn take_header(&mut self) -> Option<Metadata> {
        self.ensure_finished().await;
        self.cell.as_mut().ops.op3.take_metadata()
    }

    pub async fn recv_message(&mut self) -> Result<Resp> {
        self.ensure_finished().await;
        let status = self.cell.as_mut().ops.op5.take_status().unwrap();
        if status.code() == RpcStatusCode::OK {
            let data = unsafe { self.cell.as_mut().ops.op4.take_message().unwrap() };
            (self.resp_de)(GrpcByteBufferReader::new(data))
        } else {
            Err(Error::RpcFailure(status))
        }
    }

    pub async fn take_trailer(&mut self) -> Option<Metadata> {
        self.ensure_finished().await;
        self.cell.as_mut().ops.op5.take_metadata()
    }
}

struct Streaming {
    call: Call,
    read_tag: Tag<Streaming>,
    read_ops: CallOpSet<RecvInitialMetadata, RecvMessage, ClientRecvStatus>,
    write_tag: Tag<Streaming>,
    write_ops: CallOpSet<SendInitialMetadata, SendMessage, ClientSendClose>,
}

pub struct Reader<Resp> {
    cell: RefCallCell<Streaming>,
    resp_de: DeserializeFn<Resp>,
}

pub struct Writer<Req> {
    cell: RefCallCell<Streaming>,
    req_ser: SerializeFn<Req>,
}

pub fn client_streaming<Req, Resp>(channel: &Channel, method: &Method<Req, Resp>, mut opt: CallOption)
-> Result<(Writer<Req>, Reader<Resp>)> 
{
    let call = channel.create_call(method, &opt)?;
    let cell = unsafe {
        RefCallCell::new_in(call.call, move |im: &mut MaybeUninit<Streaming>, ptr| {
            unsafe {
                let l = im.as_mut_ptr();
                addr_of_mut!((*l).read_tag).write(Tag::new(ptr));
                addr_of_mut!((*l).read_ops.op0).write(SendInitialMetadata::new(opt.headers, opt.call_flags));
                addr_of_mut!((*l).read_ops.op1).write(Default::default());
                addr_of_mut!((*l).read_ops.op2).write(Default::default());
                addr_of_mut!((*l).read_ops.op3).write(Default::default());
                addr_of_mut!((*l).read_ops.resolved).write(false);
                addr_of_mut!((*l).write_tag).write(Tag::new(ptr));
                addr_of_mut!((*l).write_ops.op0).write(Default::default());
                addr_of_mut!((*l).write_ops.op1).write(Default::default());
                addr_of_mut!((*l).write_ops.resolved).write(false);
                let p = (*l).read_tag.get_tag();
                (*l).read_ops.call(call.call, p);
                addr_of_mut!((*l).call).write(call);
            }
        })
    };
    Ok((Writer {
        cell: cell.clone(),
        req_ser: method.req_ser(),
    }, Reader {
        cell,
        resp_de: method.resp_de(),
    }))
}

impl<Resp> Reader<Resp> {
    async fn ensure_finished(&mut self) {
        if !self.cell.as_mut().read_ops.resolved {
            let b = (&mut self.cell.as_mut().read_tag).await;
            assert!(b);
            self.cell.as_mut().read_ops.finalize_result(b);
        }
    }

    pub async fn take_header(&mut self) -> Option<Metadata> {
        self.ensure_finished().await;
        self.cell.as_mut().read_ops.op1.take_metadata()
    }

    pub async fn recv_message(&mut self) -> Result<Resp> {
        self.ensure_finished().await;
        let status = self.cell.as_mut().read_ops.op3.take_status().unwrap();
        if status.code() == RpcStatusCode::OK {
            let data = unsafe { self.cell.as_mut().read_ops.op2.take_message().unwrap() };
            (self.resp_de)(GrpcByteBufferReader::new(data))
        } else {
            Err(Error::RpcFailure(status))
        }
    }

    pub async fn take_trailer(&mut self) -> Option<Metadata> {
        self.ensure_finished().await;
        self.cell.as_mut().read_ops.op3.take_metadata()
    }
}

impl<Req> Writer<Req> {
    fn check_ready(&mut self) -> Option<bool> {
        let cell = self.cell.as_mut();
        if !cell.write_ops.op0.has_pending_bytes() {
            return Some(true);
        }
        let res = cell.write_tag.ready();
        if let Some(v) = res {
            cell.write_ops.finalize_result(v);
        }
        res
    }

    pub fn flush_header(&mut self) -> Option<bool> {
        
        match self.check_ready() {
            Some(true) => (),
            res => return res,
        }

    }

    pub fn write(&mut self, req: &Req) -> Option<bool> {
        self.write_with_flags(req, WriteFlags::default())
    }

    pub fn write_with_flags(&mut self, req: &Req, flags: WriteFlags) -> Option<bool> {
        match self.check_ready() {
            Some(true) => (),
            res => return res,
        }
        let cell = self.cell.as_mut();
        if cell.write_ops.op1.closed() {
            return Some(false);
        }

        let mut payload = GrpcSlice::default();
        (self.req_ser)(req, &mut payload);
        let buf = GrpcByteBuffer::from(&payload);
        cell.write_ops.op0.send_message(buf, flags);
        let p = cell.write_tag.get_tag();
        cell.write_ops.call(cell.call.call, p);
        Some(true)
    }

    pub async fn wait_for_ready(&mut self) -> bool {
        let cell = self.cell.as_mut();
        if !cell.write_ops.op0.has_pending_bytes() {
            return true;
        }
        let v = (&mut cell.write_tag).await;
        cell.write_ops.finalize_result(v);
        v
    }

    pub fn close(&mut self) -> Option<bool> {
        match self.check_ready() {
            Some(true) => (),
            res => return res,
        }
        let cell = self.cell.as_mut();
        cell.write_ops.op1.mark_closed();
        let p = cell.write_tag.get_tag();
        cell.write_ops.call(cell.call.call, p);
        Some(true)
    }
}
