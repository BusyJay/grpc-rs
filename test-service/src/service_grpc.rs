// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_TEST_SERVICE_DATA_STREAM: ::grpcio::Method<super::service::Data, super::service::Data> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Duplex,
    name: "/TestService/DataStream",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct TestServiceClient {
    client: ::grpcio::Client,
}

impl TestServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        TestServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn data_stream_opt(&self, opt: ::grpcio::CallOption) -> ::grpcio::Result<(::grpcio::ClientDuplexSender<super::service::Data>, ::grpcio::ClientDuplexReceiver<super::service::Data>)> {
        self.client.duplex_streaming(&METHOD_TEST_SERVICE_DATA_STREAM, opt)
    }

    pub fn data_stream(&self) -> ::grpcio::Result<(::grpcio::ClientDuplexSender<super::service::Data>, ::grpcio::ClientDuplexReceiver<super::service::Data>)> {
        self.data_stream_opt(::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait TestService {
    fn data_stream(&mut self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::service::Data>, sink: ::grpcio::DuplexSink<super::service::Data>);
}

pub fn create_test_service<S: TestService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s;
    builder = builder.add_duplex_streaming_handler(&METHOD_TEST_SERVICE_DATA_STREAM, move |ctx, req, resp| {
        instance.data_stream(ctx, req, resp)
    });
    builder.build()
}
