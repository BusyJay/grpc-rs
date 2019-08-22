use grpcio::*;
use test_service::service_grpc::*;
use test_service::service::*;
use std::sync::Arc;
use std::sync::atomic::*;
use futures::*;
use std::time::Duration;

fn main() {
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).initial_reconnect_backoff(Duration::from_millis(100)).max_reconnect_backoff(Duration::from_millis(500)).
    connect("127.0.0.1:7777");

    let client = TestServiceClient::new(channel);
    let (mut tx, mut rx) = client.data_stream().unwrap();
    let backlog = Arc::new(AtomicUsize::new(0));
    let backlog2 = backlog.clone();
    let t1 = std::thread::spawn(move || {
        let bytes = [b'X'; 200];
        let mut data = Data::new();
        data.set_Payload(bytes.to_vec());
        loop {
            if backlog.load(Ordering::SeqCst) < 50000 {
                tx = match tx.send((data.clone(), WriteFlags::default())).wait() {
                    Ok(t) => t,
                    Err(e) => {
                        println!("err {:?}", e);
                        break;
                    }
                };
                backlog.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    let t2 = std::thread::spawn(move || {
        loop {
            if backlog2.load(Ordering::SeqCst) > 0 {
                rx = match rx.into_future().wait() {
                    Ok((Some(_), rx)) => rx,
                    Ok((None, _)) => break,
                    Err((e, r)) => {
                        println!("{:?}", e);
                        r
                    }
                };
                backlog2.fetch_sub(1, Ordering::SeqCst);
            }
        }
    });
    t1.join().unwrap();
    t2.join().unwrap();
}