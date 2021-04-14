// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use futures::task::{Waker, Context, Poll};
use futures::prelude::*;
use std::pin::Pin;
use std::mem::{ManuallyDrop, self, MaybeUninit};
use parking_lot::Mutex;
use grpcio_sys::{grpc_call, gpr_refcount};
use std::ffi::c_void;
use std::ptr::{self, addr_of_mut};

pub(crate) struct RefCallCellInner<Provider> {
    ref_counted: gpr_refcount,
    provider: Provider,
}

impl<Provider> RefCallCellInner<Provider> {
    pub fn add_ref(&mut self) {
        unsafe {
            grpcio_sys::gpr_ref(&mut self.ref_counted)
        }
    }

    pub unsafe fn unref(&mut self) {
        unsafe {
            if grpcio_sys::gpr_unref(&mut self.ref_counted) != 0 {
                ptr::drop_in_place(&mut self.provider);
            }
        }
    }
}

pub(crate) struct RefCallCell<Provider> {
    ptr: *mut RefCallCellInner<Provider>,
}

impl<Provider> RefCallCell<Provider> {
    pub unsafe fn new_in(call: *mut grpc_call, init: impl FnOnce(&mut MaybeUninit<Provider>, *mut RefCallCellInner<Provider>)) -> RefCallCell<Provider> {
        let ptr = grpcio_sys::grpc_call_arena_alloc(call, mem::size_of::<RefCallCellInner<MaybeUninit<Provider>>>()) as *mut RefCallCellInner<MaybeUninit<Provider>>;
        grpcio_sys::gpr_ref_init(addr_of_mut!((*ptr).ref_counted), 1);
        init(&mut (*ptr).provider, ptr as _);

        RefCallCell {
            ptr: ptr as _,
        }
    }

    pub fn as_ref(&self) -> &Provider {
        unsafe { &(*self.ptr).provider }
    }

    pub fn as_mut(&mut self) -> &mut Provider {
        unsafe { &mut (*self.ptr).provider }
    }
}

impl<Provider> Clone for RefCallCell<Provider> {
    fn clone(&self) -> RefCallCell<Provider> {
        unsafe { (*self.ptr).add_ref(); }
        RefCallCell {
            ptr: self.ptr,
        }
    }
}

impl<Provider> Drop for RefCallCell<Provider> {
    fn drop(&mut self) {
        unsafe {
            (*self.ptr).unref()
        }
    }
}

enum Completion {
    Waker(Waker),
    Empty,
    Value(bool),
}

pub(crate) struct Tag<Provider> {
    inner: *mut RefCallCellInner<Provider>,
    completion: Mutex<Completion>,
}

impl<Provider> Tag<Provider> {
    pub fn new(inner: *mut RefCallCellInner<Provider>) -> Tag<Provider> {
        Tag {
            inner,
            completion: Mutex::new(Completion::Empty),
        }
    }
}

impl<Provider> Tag<Provider> {
    /// Get a raw tag for completion queue.
    ///
    /// The returned tag must be called `set_result` to avoid memory leak.
    pub fn get_tag(&mut self) -> *mut c_void {
        // The tag is designed to be store in arena of grpc_call, so
        // ref the call.
        let mut completion = self.completion.lock();
        match mem::replace(&mut *completion, Completion::Empty) {
            Completion::Empty | Completion::Value(_) => (),
            Completion::Waker(_) => panic!("Only one ongoing tag is allowed"),
        }
        drop(completion);
        unsafe { (*self.inner).add_ref(); }
        self as *mut _ as _
    }
}

impl<Provider> Future for Tag<Provider> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<bool> {
        let mut completion = self.completion.lock();
        match &*completion {
            Completion::Value(b) => return Poll::Ready(*b),
            Completion::Waker(waker) => if cx.waker().will_wake(waker) {
                return Poll::Pending;
            }
            _ => ()
        }
        *completion = Completion::Waker(cx.waker().clone());
        Poll::Pending
    }
}

impl<Provider> Tag<Provider> {
    pub fn set_result(&mut self, b: bool) {
        let mut completion = self.completion.lock();
        match mem::replace(&mut *completion, Completion::Value(b)) {
            Completion::Waker(waker) => waker.wake(),
            Completion::Empty => (),
            Completion::Value(_) => panic!("completion tag can't be resolved twice."),
        }
        unsafe { (*self.inner).unref() }
    }

    pub fn ready(&self) -> Option<bool> {
        let mut completion = self.completion.lock();
        if let Completion::Value(v) = &*completion {
            Some(*v)
        } else {
            None
        }
    }
}
