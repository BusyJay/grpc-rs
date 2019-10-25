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

use crate::call::{RpcStatus, RpcStatusCode};
use crate::error::{Error, Result};
use futures::task::{self, Task};
use futures::{Async, Future};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

const LOCK: u8 = 1;
const FUTURE_LINKED: u8 = 2;
const HOLDER_LINKED: u8 = 4;
const STATE_OFF: u8 = 3;
const BIT_MASK: u8 = (1 << STATE_OFF) - 1;
const STATE_MASK: u8 = 255 - BIT_MASK;
const UNINIT: u8 = 1 << STATE_OFF;
const WAIT: u8 = 2 << STATE_OFF;
const SET: u8 = 3 << STATE_OFF;
const READ: u8 = 4 << STATE_OFF;

struct PairInner<T> {
    state: AtomicU8,
    result: Option<Result<T>>,
    task: Option<Task>,
}

pub struct PairFuture<T> {
    inner: *mut PairInner<T>,
}

impl<T> Future for PairFuture<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<T>> {
        let i = unsafe { &mut *self.inner };
        let mut state = i.state.load(Ordering::SeqCst);
        if state & STATE_MASK == UNINIT {
            i.task = Some(task::current());
        }
        loop {
            if state & STATE_MASK == SET {
                break;
            }
            if state & STATE_MASK == UNINIT {
                let new_state = WAIT | (state & BIT_MASK);
                match i.state.compare_exchange_weak(
                    state,
                    new_state,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(Async::NotReady),
                    Err(s) => state = s,
                }
                assert_ne!(state & STATE_MASK, WAIT);
            } else if state & STATE_MASK == WAIT {
                match i.state.compare_exchange_weak(
                    state,
                    state | LOCK,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        if !i.task.as_ref().unwrap().will_notify_current() {
                            i.task = Some(task::current());
                        }
                        loop {
                            match i.state.compare_exchange_weak(
                                state | LOCK,
                                state,
                                Ordering::SeqCst,
                                Ordering::Acquire,
                            ) {
                                Ok(_) => break,
                                Err(s) => state = (s & STATE_MASK) | (s & (BIT_MASK - LOCK)),
                            }
                        }
                        if state & STATE_MASK == SET {
                            break;
                        }
                        return Ok(Async::NotReady);
                    }
                    Err(s) => state = s,
                }
            } else {
                panic!("unexpected state {}", state);
            }
        }

        loop {
            let new_state = READ | (state & BIT_MASK);
            match i.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(s) => state = s,
            }
        }
        Ok(Async::Ready(i.result.take().unwrap()?))
    }
}

impl<T> Drop for PairFuture<T> {
    fn drop(&mut self) {
        let i = unsafe { &mut *self.inner };
        let mut state = i.state.load(Ordering::SeqCst);
        loop {
            if state & HOLDER_LINKED == 0 {
                unsafe { Box::from_raw(self.inner) };
                return;
            }
            if state & STATE_MASK == SET {
                i.result.take();
            }
            match i.state.compare_exchange_weak(
                state,
                state - FUTURE_LINKED,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(s) => state = s,
            }
        }
    }
}

unsafe impl<T: Send> Send for PairFuture<T> {}

/// Get the future status without the need to poll.
///
/// If the future is polled successfully, this function will return None.
/// Not implemented as method as it's only for internal usage.
pub fn check_alive<T>(f: &mut PairFuture<T>) -> Result<()> {
    let i = unsafe { &mut *f.inner };
    let s = i.state.load(Ordering::SeqCst);
    if s & STATE_MASK != SET {
        return Ok(());
    }
    match i.result {
        None => Ok(()),
        Some(Err(Error::RpcFailure(ref status))) => {
            Err(Error::RpcFinished(Some(status.to_owned())))
        }
        Some(Ok(_)) | Some(Err(_)) => Err(Error::RpcFinished(None)),
    }
}

pub struct PairHolder<T> {
    inner: *mut PairInner<T>,
}

impl<T> PairHolder<T> {
    pub fn set_result(&mut self, result: Result<T>) {
        let i = unsafe { &mut *self.inner };
        let mut state = i.state.load(Ordering::SeqCst);
        if state & FUTURE_LINKED == 0 {
            return;
        }
        i.result = Some(result);
        loop {
            if state & STATE_MASK == SET || state & STATE_MASK == READ {
                panic!("can't set result twice!");
            }
            let new_state = SET | (state & BIT_MASK);
            match i.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(s) => state = s,
            }
            if state & FUTURE_LINKED == 0 {
                i.result.take();
                i.task.take();
                return;
            }
        }

        // future part can be in following situations:
        // 0. Have not been polled yet, it will fetch the result directly once polling.
        // 1. Setting task the first time, so state should be uninit. It will check whether
        //    the state is still uninit before return, which will be obvious false.
        // 2. The task is set and future is yield. Then we need to notify it. Note that we don't
        //    need to get the lock before notifying. Because if future tries to set task second
        //    time, it need to get the lock first, in which case it will find the updated state
        //    and fetch the result directly.
        // 3. The task is set and polled the second time. In this case, state should be locked.
        //    When it tries to get the lock or release the lock, it will find the updated state
        //    and then fetch the result.
        // 4. The task is set and second poll is finished. It's the same as 2.
        // 5. The future is dropped.
        // Obviously we only need to handle task at situation 2, 4 and 5. In all those cases,
        // lock at Holder side is unnecessary.
        if state & STATE_MASK == UNINIT || state & LOCK == LOCK {
            return;
        }
        i.task.take().unwrap().notify();
    }
}

impl<T> Drop for PairHolder<T> {
    fn drop(&mut self) {
        let i = unsafe { &mut *self.inner };
        let mut state = i.state.load(Ordering::SeqCst);
        loop {
            if state & FUTURE_LINKED == 0 {
                unsafe { Box::from_raw(self.inner) };
                return;
            }
            if state & STATE_MASK == WAIT {
                let status = RpcStatus::new(
                    RpcStatusCode::CANCELLED,
                    Some("pair dropped without settting result.".to_owned()),
                );
                self.set_result(Err(Error::RpcFailure(status)));
            }
            match i.state.compare_exchange_weak(
                state,
                state - HOLDER_LINKED,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(s) => state = s,
            }
        }
    }
}

pub fn pair<T>() -> (PairFuture<T>, PairHolder<T>) {
    let inner = Box::into_raw(Box::new(PairInner {
        state: AtomicU8::new(UNINIT | FUTURE_LINKED | HOLDER_LINKED),
        result: None,
        task: None,
    }));
    (PairFuture { inner }, PairHolder { inner })
}

/// A simple spin lock for synchronization between Promise
/// and future.
pub struct SpinLock<T> {
    handle: UnsafeCell<T>,
    lock: AtomicBool,
}

// It's a lock, as long as the content can be sent between
// threads, it's Sync and Send.
unsafe impl<T: Send> Sync for SpinLock<T> {}
unsafe impl<T: Send> Send for SpinLock<T> {}

impl<T> SpinLock<T> {
    /// Create a lock with the given value.
    pub fn new(t: T) -> SpinLock<T> {
        SpinLock {
            handle: UnsafeCell::new(t),
            lock: AtomicBool::new(false),
        }
    }

    pub fn lock(&self) -> LockGuard<'_, T> {
        // TODO: what if poison?
        // It's safe to use swap here. If previous is false, then the lock
        // is taken, loop will break, set it to true is expected;
        // If previous is true, then the loop will go on until others swap
        // back a false, set it to true changes nothing.
        while self.lock.swap(true, Ordering::SeqCst) {}
        LockGuard { inner: self }
    }
}

/// A guard for `SpinLock`.
pub struct LockGuard<'a, T> {
    inner: &'a SpinLock<T>,
}

impl<'a, T> Deref for LockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.inner.handle.get() }
    }
}

impl<'a, T> DerefMut for LockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.handle.get() }
    }
}

impl<'a, T> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        self.inner.lock.swap(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::*;
    use std::sync::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_lock() {
        let lock1 = Arc::new(SpinLock::new(2));
        let lock2 = lock1.clone();
        let (tx, rx) = mpsc::channel();
        let guard = lock1.lock();
        thread::spawn(move || {
            let _guard = lock2.lock();
            tx.send(()).unwrap();
        });
        thread::sleep(Duration::from_millis(10));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(guard);
        assert_eq!(rx.recv(), Ok(()));
    }
}
