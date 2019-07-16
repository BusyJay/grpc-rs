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

use std::collections::HashMap;
use std::fmt::Write;
use std::io::{self, Read, Write as IoWrite};
use prost::Message;
use prost_types::*;

pub struct GenResult {
    pub name: String,
    pub content: Vec<u8>,
}

use super::util::{self, fq_grpc, to_snake_case, MethodType};


pub fn proto_name_to_mod(name: &str) -> String {
    let mut base = name.rsplit("/").next().unwrap();
    if base.ends_with(".proto") {
        base = &base[..base.len() - 6];
    }
    base.to_lowercase().replace('-', "_")
}

pub fn resolve_type(base: &str, mut name: &str) -> String {
    if name.starts_with(".") {
        name = &name[1..];
    }
    if name.contains('.') {
        name.replace('.', "::")
    } else {
        format!("{}::{}", base, name)
    }
}

struct MethodGen<'a> {
    base: &'a str,
    proto: &'a MethodDescriptorProto,
    service_name: String,
    service_path: String,
}

impl<'a> MethodGen<'a> {
    fn new(
        base: &'a str,
        proto: &'a MethodDescriptorProto,
        service_name: String,
        service_path: String,
    ) -> MethodGen<'a> {
        MethodGen {
            base,
            proto,
            service_name,
            service_path,
        }
    }

    fn input(&self) -> String {
        format!(
            "super::{}",
            resolve_type(&self.base, self.proto.input_type())
        )
    }

    fn output(&self) -> String {
        format!(
            "super::{}",
            resolve_type(&self.base, self.proto.output_type())
        )
    }

    fn method_type(&self) -> (MethodType, String) {
        match (
            self.proto.client_streaming(),
            self.proto.server_streaming(),
        ) {
            (false, false) => (MethodType::Unary, fq_grpc("MethodType::Unary")),
            (true, false) => (
                MethodType::ClientStreaming,
                fq_grpc("MethodType::ClientStreaming"),
            ),
            (false, true) => (
                MethodType::ServerStreaming,
                fq_grpc("MethodType::ServerStreaming"),
            ),
            (true, true) => (MethodType::Duplex, fq_grpc("MethodType::Duplex")),
        }
    }

    fn service_name(&self) -> String {
        to_snake_case(&self.service_name)
    }

    fn name(&self) -> String {
        to_snake_case(self.proto.name())
    }

    fn fq_name(&self) -> String {
        format!("\"{}/{}\"", self.service_path, &self.proto.name())
    }

    fn const_method_name(&self) -> String {
        format!(
            "METHOD_{}_{}",
            self.service_name().to_uppercase(),
            self.name().to_uppercase()
        )
    }

    fn write_definition(&self, w: &mut String) {
        let head = format!(
            "const {}: {}<{}, {}> = {} {{",
            self.const_method_name(),
            fq_grpc("Method"),
            self.input(),
            self.output(),
            fq_grpc("Method")
        );
        let pb_mar = format!(
            "{} {{ ser: {}, de: {} }}",
            fq_grpc("Marshaller"),
            fq_grpc("pr_ser"),
            fq_grpc("pr_de")
        );
        writeln!(w, "
{}
    ty: {},
    name: {},
    req_mar: {},
    resp_mar: {},
}};
", head, self.method_type().1, self.fq_name(), &pb_mar, &pb_mar).unwrap();
    }

    // Method signatures
    fn unary(&self, method_name: &str) -> String {
        format!(
            "{}(&self, req: &{}) -> {}<{}>",
            method_name,
            self.input(),
            fq_grpc("Result"),
            self.output()
        )
    }

    fn unary_opt(&self, method_name: &str) -> String {
        format!(
            "{}_opt(&self, req: &{}, opt: {}) -> {}<{}>",
            method_name,
            self.input(),
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            self.output()
        )
    }

    fn unary_async(&self, method_name: &str) -> String {
        format!(
            "{}_async(&self, req: &{}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("Result"),
            fq_grpc("ClientUnaryReceiver"),
            self.output()
        )
    }

    fn unary_async_opt(&self, method_name: &str) -> String {
        format!(
            "{}_async_opt(&self, req: &{}, opt: {}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("ClientUnaryReceiver"),
            self.output()
        )
    }

    fn client_streaming(&self, method_name: &str) -> String {
        format!(
            "{}(&self) -> {}<({}<{}>, {}<{}>)>",
            method_name,
            fq_grpc("Result"),
            fq_grpc("ClientCStreamSender"),
            self.input(),
            fq_grpc("ClientCStreamReceiver"),
            self.output()
        )
    }

    fn client_streaming_opt(&self, method_name: &str) -> String {
        format!(
            "{}_opt(&self, opt: {}) -> {}<({}<{}>, {}<{}>)>",
            method_name,
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("ClientCStreamSender"),
            self.input(),
            fq_grpc("ClientCStreamReceiver"),
            self.output()
        )
    }

    fn server_streaming(&self, method_name: &str) -> String {
        format!(
            "{}(&self, req: &{}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("Result"),
            fq_grpc("ClientSStreamReceiver"),
            self.output()
        )
    }

    fn server_streaming_opt(&self, method_name: &str) -> String {
        format!(
            "{}_opt(&self, req: &{}, opt: {}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("ClientSStreamReceiver"),
            self.output()
        )
    }

    fn duplex_streaming(&self, method_name: &str) -> String {
        format!(
            "{}(&self) -> {}<({}<{}>, {}<{}>)>",
            method_name,
            fq_grpc("Result"),
            fq_grpc("ClientDuplexSender"),
            self.input(),
            fq_grpc("ClientDuplexReceiver"),
            self.output()
        )
    }

    fn duplex_streaming_opt(&self, method_name: &str) -> String {
        format!(
            "{}_opt(&self, opt: {}) -> {}<({}<{}>, {}<{}>)>",
            method_name,
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("ClientDuplexSender"),
            self.input(),
            fq_grpc("ClientDuplexReceiver"),
            self.output()
        )
    }

    fn write_client(&self, w: &mut String) {
        let method_name = self.name();
        match self.method_type().0 {
            // Unary
            MethodType::Unary => {
                writeln!(w, "
    pub fn {} {{
        self.clinet.unary_call(&{}, req, opt)
    }}

    pub fn {} {{
        self.{}_opt(req, ::grpcio::CallOptions::default())
    }}

    pub fn {} {{
        self.client.unary_call_async(&{}, req, opt)
    }}

    pub fn {} {{
        self.{}_async_opt(req, ::grpcio::CallOptions::default())
    }}
", self.unary_opt(&method_name), self.const_method_name(), self.unary(&method_name), method_name,
self.unary_async_opt(&method_name), self.const_method_name(), &self.unary_async(&method_name), method_name).unwrap();
            }

            // Client streaming
            MethodType::ClientStreaming => {
                writeln!(w, "
    pub fn {} {{
        self.client.client_streaming(&{}, opt)
    }}

    pub fn {} {{
        self.{}_opt(::grpcio::CallOptions::default())
    }}
", self.client_streaming_opt(&method_name), self.const_method_name(), self.client_streaming(&method_name), method_name).unwrap();
            }

            // Server streaming
            MethodType::ServerStreaming => {
                writeln!(w, "
    pub fn {} {{
        self.client.server_streaming(&{}, req, opt)
    }}

    pub fn {} {{
        self.{}_opt(req, ::grpcio::CallOption::default())
    }}
", self.server_streaming_opt(&method_name), self.const_method_name(), self.server_streaming(&method_name), method_name).unwrap();
            }

            // Duplex streaming
            MethodType::Duplex => {
                writeln!(w, "
    pub fn {} {{
        self.client.duplex_streaming(&{}, opt)
    }}

    pub fn {} {{
        self.{}_opt(::grpcio::CallOptions::default())
    }}
", self.duplex_streaming_opt(&method_name), self.const_method_name(), self.duplex_streaming(&method_name), method_name).unwrap();
            }
        };
    }

    fn write_service(&self, w: &mut String) {
        let req_stream_type = format!("{}<{}>", fq_grpc("RequestStream"), self.input());
        let (req, req_type, resp_type) = match self.method_type().0 {
            MethodType::Unary => ("req", self.input(), "UnarySink"),
            MethodType::ClientStreaming => ("stream", req_stream_type, "ClientStreamingSink"),
            MethodType::ServerStreaming => ("req", self.input(), "ServerStreamingSink"),
            MethodType::Duplex => ("stream", req_stream_type, "DuplexSink"),
        };
        let sig = format!(
            "{}(&mut self, ctx: {}, {}: {}, sink: {}<{}>)",
            self.name(),
            fq_grpc("RpcContext"),
            req,
            req_type,
            fq_grpc(resp_type),
            self.output()
        );
        writeln!(w, "fn {};", sig).unwrap();
    }

    fn write_bind(&self, w: &mut String) {
        let add = match self.method_type().0 {
            MethodType::Unary => "add_unary_handler",
            MethodType::ClientStreaming => "add_client_streaming_handler",
            MethodType::ServerStreaming => "add_server_streaming_handler",
            MethodType::Duplex => "add_duplex_streaming_handler",
        };
        writeln!(w, "
    builder = builder.{}(&{}, move |ctx, req, resp| {{
        instance.{}(ctx, req, resp)
    }});
", add, self.const_method_name(), self.name()).unwrap();
    }
}

struct ServiceGen<'a> {
    proto: &'a ServiceDescriptorProto,
    methods: Vec<MethodGen<'a>>,
}

impl<'a> ServiceGen<'a> {
    fn new(
        base: &'a str,
        proto: &'a ServiceDescriptorProto,
        file: &FileDescriptorProto,
    ) -> ServiceGen<'a> {
        let service_path = if file.package().is_empty() {
            format!("/{}", proto.name())
        } else {
            format!("/{}.{}", file.package(), proto.name())
        };
        let methods = proto
            .method
            .iter()
            .map(|m| {
                MethodGen::new(
                    base,
                    m,
                    util::to_camel_case(proto.name()),
                    service_path.clone(),
                )
            })
            .collect();

        ServiceGen { proto, methods }
    }

    fn service_name(&self) -> String {
        util::to_camel_case(self.proto.name())
    }

    fn client_name(&self) -> String {
        format!("{}Client", self.service_name())
    }

    fn write_client(&self, w: &mut String) {
        writeln!(w, "#[derive(Clone)]").unwrap();
        writeln!(w, "
pub struct {} {{
    client: ::grpcio::Client,
}}
", self.client_name()).unwrap();

        writeln!(w, "").unwrap();

        writeln!(w, "
impl {} {{
    pub fn new(channel: ::grpcio::Channel) -> Self {{
        Self {{
            client: ::grpcio::Client::new(channel),
        }}
    }}
", self.client_name()).unwrap();

        for method in &self.methods {
            writeln!(w, "").unwrap();
            method.write_client(w);
        }
        
        writeln!(w, "
    pub fn spawn<F>(&self, f: F)
    where
        F: ::futures::Future<Item = (), Error = ()> + Send + 'static
    {{
        self.client.spawn(f)
    }}
}}
").unwrap();
    }

    fn write_server(&self, w: &mut String) {
        writeln!(w, "pub trait {} {{\n", self.service_name()).unwrap();
        for method in &self.methods {
            method.write_service(w);
        }
        writeln!(w, "}}").unwrap();
        writeln!(w, "
pub fn create_{}<S: {} + Send + Clone + 'static>(s: S) -> ::grpcio::Service {{
    let mut builder = ::grpcio::ServiceBuilder::new();
", to_snake_case(&self.service_name()), self.service_name()).unwrap();
        for method in &self.methods[0..self.methods.len() - 1] {
            writeln!(w, "    let mut instance = s.clone();").unwrap();
            method.write_bind(w);
        }

        writeln!(w, "    let mut instance = s;").unwrap();
        self.methods[self.methods.len() - 1].write_bind(w);

        writeln!(w, "    builder.build()}}").unwrap();
    }

    fn write_method_definitions(&self, w: &mut String) {
        for (i, method) in self.methods.iter().enumerate() {
            if i != 0 {
                writeln!(w, "").unwrap();
            }

            method.write_definition(w);
        }
    }

    fn write(&self, w: &mut String) {
        self.write_method_definitions(w);
        writeln!(w, "").unwrap();
        self.write_client(w);
        writeln!(w, "").unwrap();
        self.write_server(w);
    }
}

fn gen_file(
    file: &FileDescriptorProto,
) -> std::option::Option<GenResult> {
    if file.service.is_empty() {
        return None;
    }

    let base = proto_name_to_mod(file.name());

    let mut buf = String::new();
    {
        write!(&mut buf, "
// This file is generated by grpcio {}. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

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
", env!("CARGO_PKG_VERSION")).unwrap();

        for service in &file.service {
            writeln!(&mut buf, "").unwrap();
            ServiceGen::new(&base, service, file).write(&mut buf);
        }
    }

    Some(GenResult {
        name: base + "_grpc.rs",
        content: buf.into_bytes(),
    })
}

pub fn gen(
    file_descriptors: &[FileDescriptorProto],
    files_to_generate: &[String],
) -> Vec<GenResult> {
    let files_map: HashMap<&str, &FileDescriptorProto> =
        file_descriptors.iter().map(|f| (f.name(), f)).collect();

    let mut results = Vec::new();

    for file_name in files_to_generate {
        let file = files_map[&file_name[..]];

        if file.service.is_empty() {
            continue;
        }

        results.extend(gen_file(file).into_iter());
    }

    results
}

pub fn protoc_gen_grpc_rust_main() {
    let mut buf = Vec::new();
    io::stdin().read_to_end(&mut buf).unwrap();
    let req = prost_types::compiler::CodeGeneratorRequest::decode(&buf).unwrap();
    let mut res = gen(&req.proto_file, &req.file_to_generate);
    let mut resp = prost_types::compiler::CodeGeneratorResponse::default();
    resp.file = res
            .drain(..)
            .map(|file| {
                let mut r = prost_types::compiler::code_generator_response::File::default();
                r.name = Some(file.name);
                r.content = Some(String::from_utf8(file.content).unwrap());
                r
            })
            .collect();
    buf.clear();
    resp.encode(&mut buf).unwrap();
    io::stdout().write_all(&buf).unwrap();
}
