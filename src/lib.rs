// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

extern crate core;

use pyo3::prelude::*;

mod proto;
use crate::context::{deserialize_execution_plan, execute_partition, serialize_execution_plan};
pub use proto::generated::protobuf;

pub mod context;
pub mod planner;
pub mod query_stage;
pub mod shuffle;
pub mod utils;

/// A Python module implemented in Rust.
#[pymodule]
fn _raysql_internal(_py: Python, m: &PyModule) -> PyResult<()> {
    // register classes that can be created directly from Python code
    m.add_class::<context::PyContext>()?;
    m.add_class::<planner::PyExecutionGraph>()?;
    m.add_class::<query_stage::PyQueryStage>()?;
    m.add_function(wrap_pyfunction!(execute_partition, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_execution_plan, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_execution_plan, m)?)?;
    Ok(())
}
