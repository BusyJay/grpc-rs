use test_service::service_grpc::*;
use test_service::service::*;
use grpcio::*;
use std::sync::Arc;
use futures::*;

#[derive(Clone)]
struct Ts;

impl TestService for Ts {
    fn data_stream(&mut self, ctx: RpcContext, stream: RequestStream<Data>, sink: DuplexSink<Data>) {
        ctx.spawn(sink.send_all(stream.map(|d| (d, WriteFlags::default()))).map(|_| ()).map_err(|e| println!("{:?}", e)));
    }
}


fn main() {
    let env = Arc::new(Environment::new(2));
    let service = create_test_service(Ts);
    let mut builder = ServerBuilder::new(env).register_service(service);
    builder = builder.bind("127.0.0.1", 7777);

    let mut server = builder.build().unwrap();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    server.start();

    let _ = future::empty::<(), ()>().wait();
}
