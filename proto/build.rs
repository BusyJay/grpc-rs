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

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let modules = &[
        ("grpc/testing", "testing"),
        ("grpc/health/v1/", "health"),
        ("grpc/example", "example"),
    ];
    for (dir, package) in modules {
        let mut builder = jinkela_build::Builder::default();
        builder.include_dir("proto").out_dir(format!("{}/{}", out_dir, package));
        for result in walkdir::WalkDir::new(format!("proto/{}", dir)) {
            let dent = result.expect("Error happened when search protos");
            if !dent.file_type().is_file() {
                continue;
            }
            builder.compile_proto(format!("{}", dent.path().display()));
        }
        builder.build()
    }
}
