//! A small, reusable wrapper around rusty_v8 for loading scripts and calling JS functions.
//!
//! This module owns a V8 isolate and a single context. It provides helpers to:
//! - initialize V8 once per process
//! - execute JS source (from string or file)
//! - call a named global function with basic Rust arguments
//! - surface JS exceptions as Rust errors with message + stack
//!
//! The API keeps things simple and avoids lifetime hassles by storing a Global<Context>.
#![allow(unused)]
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::{Once, OnceLock};
use v8;

static INIT: Once = Once::new();
static PLATFORM: OnceLock<v8::SharedRef<v8::Platform>> = OnceLock::new();
static HOST_REG: OnceLock<
    Mutex<HashMap<String, Arc<dyn Fn(&[JsValue]) -> Result<JsReturn,Box<dyn std::error::Error>> + Send + Sync>>>,
> = OnceLock::new();

fn init_v8() {
    INIT.call_once(|| {
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform.clone());
        v8::V8::initialize();
        let _ = PLATFORM.set(platform);
    });
}

fn host_registry() -> &'static Mutex<
    HashMap<String, Arc<dyn Fn(&[JsValue]) -> Result<JsReturn,Box<dyn std::error::Error>> + Send + Sync>>,
> {
    HOST_REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn to_jsvalue(scope: &mut v8::HandleScope, v: v8::Local<v8::Value>) -> Option<JsValue> {
    if v.is_boolean() {
        return Some(JsValue::Bool(v.boolean_value(scope)));
    }
    if v.is_number() {
        return v.number_value(scope).map(JsValue::Number);
    }
    if v.is_string() {
        let s = v.to_string(scope)?;
        return Some(JsValue::Str(s.to_rust_string_lossy(scope)));
    }
    None
}

fn from_jsreturn<'s>(scope: &mut v8::HandleScope<'s>, r: JsReturn) -> v8::Local<'s, v8::Value> {
    match r {
        JsReturn::Text(s) => v8::String::new(scope, &s).unwrap().into(),
        JsReturn::Number(n) => v8::Number::new(scope, n).into(),
        JsReturn::Bool(b) => v8::Boolean::new(scope, b).into(),
        JsReturn::Json(v) => {
            let s = serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string());
            if let Some(js_s) = v8::String::new(scope, &s) {
                if let Some(parsed) = v8::json::parse(scope, js_s) {
                    return parsed;
                }
            }
            v8::String::new(scope, &s).unwrap().into()
        }
        JsReturn::Bytes(bytes) => {
            let ab = v8::ArrayBuffer::new(scope, bytes.len());
            {
                let bs = ab.get_backing_store();
                if let Some(ptr) = bs.data() {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            bytes.as_ptr(),
                            ptr.as_ptr() as *mut u8,
                            bytes.len(),
                        );
                    }
                }
            }
            v8::Uint8Array::new(scope, ab, 0, bytes.len())
                .unwrap()
                .into()
        }
    }
}

fn host_fn_dispatch(
    scope: &mut v8::HandleScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue,
) {
    // Expect first argument to be the registry key string: "module:name"
    if args.length() == 0 {
        rv.set(from_jsreturn(scope, JsReturn::Text("missing key".into())));
        return;
    }
    let key_val = args.get(0);
    let key = match key_val.to_string(scope) {
        Some(s) => s.to_rust_string_lossy(scope),
        None => {
            rv.set(from_jsreturn(scope, JsReturn::Text("invalid key".into())));
            return;
        }
    };

    let mut rust_args: Vec<JsValue> = Vec::with_capacity(args.length() as usize);
    for i in 1..args.length() {
        let v = args.get(i);
        if let Some(jv) = to_jsvalue(scope, v) {
            rust_args.push(jv);
        }
    }

    let reg = host_registry();
    let cb = reg.lock().ok().and_then(|m| m.get(&key).cloned());
    let out = match cb {
        Some(cb) => {
            cb(&rust_args).unwrap_or_else(|e| JsReturn::Text(format!("callback error: {}", e)))
        }
        None => JsReturn::Text(format!("no host callback for key: {key}")),
    };
    rv.set(from_jsreturn(scope, out));
}

fn ensure_host_dispatch_fn(cs: &mut v8::ContextScope<v8::HandleScope>) -> Result<(), Box<dyn std::error::Error>> {
    let global = cs.get_current_context().global(cs);
    let key = v8::String::new(cs, "__rust_host_call").unwrap();
    if let Some(v) = global.get(cs, key.into()) {
        if v.is_function() {
            return Ok(());
        }
    }
    let tmpl = v8::FunctionTemplate::new(cs, host_fn_dispatch);
    let f = tmpl
        .get_function(cs)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "create __rust_host_call failed"))?;
    if global.set(cs, key.into(), f.into()).is_none() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "set __rust_host_call failed").into());
    }
    Ok(())
}

/// Basic JS argument types supported for calls.
#[derive(Debug, Clone)]
pub enum JsValue {
    Str(String),
    Number(f64),
    Bool(bool),
}

impl From<&str> for JsValue {
    fn from(s: &str) -> Self {
        JsValue::Str(s.to_owned())
    }
}
impl From<String> for JsValue {
    fn from(s: String) -> Self {
        JsValue::Str(s)
    }
}
impl From<f64> for JsValue {
    fn from(n: f64) -> Self {
        JsValue::Number(n)
    }
}
impl From<i64> for JsValue {
    fn from(n: i64) -> Self {
        JsValue::Number(n as f64)
    }
}
impl From<bool> for JsValue {
    fn from(b: bool) -> Self {
        JsValue::Bool(b)
    }
}

/// Return value mapped from V8 values.
#[derive(Debug, Clone)]
pub enum JsReturn {
    /// String representation of the result (toString fallback)
    Text(String),
    /// Numeric result (f64)
    Number(f64),
    /// Boolean result
    Bool(bool),
    /// JSON value (objects/arrays serialized via JSON.stringify then parsed)
    Json(JsonValue),
    /// Raw bytes (ArrayBuffer or Uint8Array)
    Bytes(Vec<u8>),
}

pub struct V8Engine {
    isolate: v8::OwnedIsolate,
    context: v8::Global<v8::Context>,
}

impl V8Engine {
    /// Create a new engine with a fresh isolate and context.
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        init_v8();

        let isolate = v8::Isolate::new(Default::default());
        let mut isolate = isolate;

        // Create context and promote to Global so we can use it across calls.
        let context_global = {
            let mut hs = v8::HandleScope::new(&mut isolate);
            let context = v8::Context::new(&mut hs, Default::default());
            v8::Global::new(&mut hs, context)
        };

        Ok(Self {
            isolate,
            context: context_global,
        })
    }

    /// Execute a JS source string inside the engine's context.
    pub fn exec_script(&mut self, source: &str, _name: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        let mut hs = v8::HandleScope::new(&mut self.isolate);
        let local_ctx = v8::Local::new(&mut hs, &self.context);
        let mut cs = v8::ContextScope::new(&mut hs, local_ctx);
        let mut tc = v8::TryCatch::new(&mut cs);

        let code = v8::String::new(&mut tc, source)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "failed to create v8 string from source"))?;

        let script =
            v8::Script::compile(&mut tc, code, None)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "compile failed"))?;

        script
            .run(&mut tc)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "run failed").into())
            .map(|_| ())
    }

    /// Execute a JS file by path.
    pub fn exec_file(&mut self, path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let src = fs::read_to_string(path).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("read js file failed: {}: {}", path.display(), e),
            )
        })?;
        self.exec_script(&src, path.file_name().and_then(|s| s.to_str()))
    }

    /// Register a Rust function as an import for WebAssembly under a module and function name.
    /// The function will be available at `globalThis.__host_imports[module][fn_name]`.
    pub fn register_import_fn<F>(
        &mut self,
        module: &str,
        fn_name: &str,
        callback: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(&[JsValue]) -> Result<JsReturn, Box<dyn std::error::Error>> + Send + Sync + 'static,
    {
        let key = format!("{}:{}", module, fn_name);
        host_registry()
            .lock()
            .unwrap()
            .insert(key.clone(), Arc::new(callback));

        let mut hs = v8::HandleScope::new(&mut self.isolate);
        let local_ctx = v8::Local::new(&mut hs, &self.context);
        let mut cs = v8::ContextScope::new(&mut hs, local_ctx);

        // Ensure native dispatcher is available
        ensure_host_dispatch_fn(&mut cs)?;

        // Create JS wrapper that forwards to __rust_host_call with the key
        let module_lit = serde_json::to_string(module).unwrap();
        let name_lit = serde_json::to_string(fn_name).unwrap();
        let key_lit = serde_json::to_string(&key).unwrap();
        let js = format!(
            "(function() {{\n  const m = {};\n  const n = {};\n  const k = {};\n  const root = (globalThis.__host_imports = globalThis.__host_imports || {{}});\n  const mod = (root[m] = root[m] || {{}});\n  mod[n] = function(...a) {{ return globalThis.__rust_host_call(k, ...a); }};\n}})();",
            module_lit, name_lit, key_lit
        );
        let code = v8::String::new(&mut cs, &js)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "alloc js str failed"))?;
        let script = v8::Script::compile(&mut cs, code, None)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "compile wrapper failed"))?;
        script
            .run(&mut cs)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "run wrapper failed"))?;
        Ok(())
    }

    /// Instantiate a WebAssembly module from raw bytes and assign it to a global name.
    ///
    /// - `global_name`: the globalThis property to store the Instance, and `${global_name}_exports` for exports.
    /// - `wasm_bytes`: the raw .wasm content
    /// - `imports_object_js`: optional JS expression string representing imports object, e.g. "{ env: { memory: new WebAssembly.Memory({initial:1}) } }"
    pub fn instantiate_wasm_global(
        &mut self,
        global_name: &str,
        wasm_bytes: &[u8],
        imports_object_js: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut hs = v8::HandleScope::new(&mut self.isolate);
        let local_ctx = v8::Local::new(&mut hs, &self.context);
        let mut cs = v8::ContextScope::new(&mut hs, local_ctx);
        let mut tc = v8::TryCatch::new(&mut cs);

        // 1) Create an ArrayBuffer with the wasm bytes and expose it to JS as a temporary global
        let ab = v8::ArrayBuffer::new(&mut tc, wasm_bytes.len());
        {
            let bs = ab.get_backing_store();
            let dst = bs
                .data()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "arraybuffer backing store has no data ptr"))?;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    wasm_bytes.as_ptr(),
                    dst.as_ptr() as *mut u8,
                    wasm_bytes.len(),
                );
            }
        }

        let global = tc.get_current_context().global(&mut tc);
        let tmp_key =
            v8::String::new(&mut tc, "__wasm_bytes_tmp")
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "alloc tmp key"))?;
        if global.set(&mut tc, tmp_key.into(), ab.into()).is_none() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "failed to set temporary wasm bytes on global").into());
        }

        // 2) Build and run a JS snippet to instantiate and store instance+exports to globals
        let imports_expr = imports_object_js.unwrap_or("globalThis.__host_imports ?? {} ");
        let js = format!(
            "(function() {{\n  const bytes = new Uint8Array(globalThis.__wasm_bytes_tmp);\n  delete globalThis.__wasm_bytes_tmp;\n  const imports = ({});\n  const mod = new WebAssembly.Module(bytes);\n  const inst = new WebAssembly.Instance(mod, imports ?? {{}});\n  globalThis[\"{}\"] = inst;\n  globalThis[\"{}_exports\"] = inst.exports;\n}})();",
            imports_expr, global_name, global_name,
        );

        let code = v8::String::new(&mut tc, &js)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "failed to create v8 string for wasm bootstrap"))?;
        let script = v8::Script::compile(&mut tc, code, None)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "compile wasm bootstrap failed"))?;
        script
            .run(&mut tc)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "run wasm bootstrap failed"))?;
        Ok(())
    }

    /// Call an exported function of a previously-instantiated WASM instance stored under `instance_global_name`.
    pub fn call_wasm_export<T: Into<JsValue> + Clone>(
        &mut self,
        instance_global_name: &str,
        export_name: &str,
        args: &[T],
    ) -> Result<JsReturn, Box<dyn std::error::Error>> {
        let mut hs = v8::HandleScope::new(&mut self.isolate);
        let local_ctx = v8::Local::new(&mut hs, &self.context);
        let mut cs = v8::ContextScope::new(&mut hs, local_ctx);
        let mut tc = v8::TryCatch::new(&mut cs);

        let global = tc.get_current_context().global(&mut tc);

        // instance object
        let inst_key = v8::String::new(&mut tc, instance_global_name)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "alloc instance name"))?;
        let inst_val = global
            .get(&mut tc, inst_key.into())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, format!("global has no wasm instance '{}': not found", instance_global_name)))?;
        let inst_obj = v8::Local::<v8::Object>::try_from(inst_val).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("wasm instance '{}' is not an object", instance_global_name),
            )
        })?;

        // exports object
        let exports_key =
            v8::String::new(&mut tc, "exports")
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "alloc 'exports'"))?;
        let exports_val = inst_obj
            .get(&mut tc, exports_key.into())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "instance.exports missing"))?;
        let exports_obj = v8::Local::<v8::Object>::try_from(exports_val)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "instance.exports is not an object"))?;

        // exported function
        let fn_key =
            v8::String::new(&mut tc, export_name)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "alloc export name"))?;
        let fn_val = exports_obj
            .get(&mut tc, fn_key.into())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, format!("export '{}' not found", export_name)))?;
        if !fn_val.is_function() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("export '{}' is not a function", export_name)).into());
        }
        let func = v8::Local::<v8::Function>::try_from(fn_val).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to cast export '{}' to Function", export_name),
            )
        })?;

        // argv
        let mut argv: Vec<v8::Local<v8::Value>> = Vec::with_capacity(args.len());
        for a in args.iter().cloned() {
            match a.into() {
                JsValue::Str(s) => {
                    let v = v8::String::new(&mut tc, &s)
                        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "arg to v8 string failed"))?;
                    argv.push(v.into());
                }
                JsValue::Number(n) => argv.push(v8::Number::new(&mut tc, n).into()),
                JsValue::Bool(b) => argv.push(v8::Boolean::new(&mut tc, b).into()),
            }
        }

        let recv = v8::undefined(&mut tc).into();
        let result = func
            .call(&mut tc, recv, &argv)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "wasm export call failed"))?;

        if result.is_boolean() {
            return Ok(JsReturn::Bool(result.boolean_value(&mut tc)));
        }
        if result.is_number() {
            let n = result
                .number_value(&mut tc)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "number_value failed"))?;
            return Ok(JsReturn::Number(n));
        }
        if result.is_uint8_array() {
            let arr = v8::Local::<v8::Uint8Array>::try_from(result)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "failed to cast to Uint8Array"))?;
            let offset = arr.byte_offset();
            let len = arr.byte_length();
            let buf = arr
                .buffer(&mut tc)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "Uint8Array.buffer() failed"))?;
            let bs = buf.get_backing_store();
            let base = bs.data().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "backing store no data"))?;
            unsafe {
                let src = std::slice::from_raw_parts(base.as_ptr().add(offset) as *const u8, len);
                return Ok(JsReturn::Bytes(src.to_vec()));
            }
        }
        if result.is_array_buffer() {
            let ab = v8::Local::<v8::ArrayBuffer>::try_from(result)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "failed to cast to ArrayBuffer"))?;
            let len = ab.byte_length();
            let bs = ab.get_backing_store();
            let ptr = bs.data().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "backing store no data"))?;
            unsafe {
                let src = std::slice::from_raw_parts(ptr.as_ptr() as *const u8, len);
                return Ok(JsReturn::Bytes(src.to_vec()));
            }
        }
        if result.is_object() || result.is_array() {
            if let Some(s) = v8::json::stringify(&mut tc, result) {
                let json_str = s.to_rust_string_lossy(&mut tc);
                return if let Ok(v) = serde_json::from_str::<JsonValue>(&json_str) {
                    Ok(JsReturn::Json(v))
                } else {
                    Ok(JsReturn::Text(json_str))
                };
            }
        }
        let s = result
            .to_string(&mut tc)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "export result.toString failed"))?;
        Ok(JsReturn::Text(s.to_rust_string_lossy(&mut tc)))
    }
    /// Call a global function with basic arguments.
    pub fn call_function<T: Into<JsValue> + Clone>(
        &mut self,
        name: &str,
        args: &[T],
    ) -> Result<JsReturn, Box<dyn std::error::Error>> {
        let mut hs = v8::HandleScope::new(&mut self.isolate);
        let local_ctx = v8::Local::new(&mut hs, &self.context);
        let mut cs = v8::ContextScope::new(&mut hs, local_ctx);
        let mut tc = v8::TryCatch::new(&mut cs);

        let global = tc.get_current_context().global(&mut tc);
        let fname = v8::String::new(&mut tc, name)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "failed to create v8 string for function name"))?;

        let func_val = global
            .get(&mut tc, fname.into())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, format!("global has no property '{}': not defined", name)))?;
        if !func_val.is_function() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("'{}' is not a function", name)).into());
        }
        let func = v8::Local::<v8::Function>::try_from(func_val).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to cast global property '{}' to Function", name),
            )
        })?;

        let mut argv: Vec<v8::Local<v8::Value>> = Vec::with_capacity(args.len());
        for a in args.iter().cloned() {
            match a.into() {
                JsValue::Str(s) => {
                    let v = v8::String::new(&mut tc, &s)
                        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "arg to v8 string failed"))?;
                    argv.push(v.into());
                }
                JsValue::Number(n) => {
                    argv.push(v8::Number::new(&mut tc, n).into());
                }
                JsValue::Bool(b) => {
                    argv.push(v8::Boolean::new(&mut tc, b).into());
                }
            }
        }

        let recv = global.into();
        let result = func
            .call(&mut tc, recv, &argv)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "function call failed"))?;

        // Map V8 value to JsReturn
        if result.is_boolean() {
            return Ok(JsReturn::Bool(result.boolean_value(&mut tc)));
        }
        if result.is_number() {
            let n = result
                .number_value(&mut tc)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "number_value failed"))?;
            return Ok(JsReturn::Number(n));
        }
        if result.is_uint8_array() {
            let arr = v8::Local::<v8::Uint8Array>::try_from(result)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "failed to cast to Uint8Array"))?;
            let offset = arr.byte_offset();
            let len = arr.byte_length();
            let buf = arr
                .buffer(&mut tc)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "Uint8Array.buffer() failed"))?;
            let bs = buf.get_backing_store();
            let base = bs.data().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "backing store no data"))?;
            unsafe {
                let src = std::slice::from_raw_parts(base.as_ptr().add(offset) as *const u8, len);
                return Ok(JsReturn::Bytes(src.to_vec()));
            }
        }
        if result.is_array_buffer() {
            let ab = v8::Local::<v8::ArrayBuffer>::try_from(result)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "failed to cast to ArrayBuffer"))?;
            let len = ab.byte_length();
            let bs = ab.get_backing_store();
            let ptr = bs.data().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "backing store no data"))?;
            unsafe {
                let src = std::slice::from_raw_parts(ptr.as_ptr() as *const u8, len);
                return Ok(JsReturn::Bytes(src.to_vec()));
            }
        }
        if result.is_object() || result.is_array() {
            if let Some(s) = v8::json::stringify(&mut tc, result) {
                let json_str = s.to_rust_string_lossy(&mut tc);
                return if let Ok(v) = serde_json::from_str::<JsonValue>(&json_str) {
                    Ok(JsReturn::Json(v))
                } else {
                    Ok(JsReturn::Text(json_str))
                };
            }
        }
        let s = result
            .to_string(&mut tc)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "result.toString failed"))?;
        Ok(JsReturn::Text(s.to_rust_string_lossy(&mut tc)))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{JsReturn, V8Engine};

    #[test]
    fn test() -> Result<(), Box<dyn std::error::Error>> {
        // Example: load script file and call a function
        let mut engine = V8Engine::new().expect("init v8");
        if let Err(e) = engine.exec_file("bdms.js") {
            eprintln!("Failed to execute bdms.js: {e}");
            std::process::exit(1);
        }

        let url = "is_h5=1&origin_type=638301&pc_client_type=1&pc_libra_divert=Mac&update_version_code=170400&version_code=&version_name=&cookie_enabled=true&screen_width=2056&screen_height=1329&browser_language=zh-CN&browser_platform=MacIntel&browser_name=Edge&browser_version=129.0.0.0&browser_online=true&engine_name=Blink&engine_version=129.0.0.0&os_name=Mac+OS&os_version=10.15.7&cpu_core_num=10&device_memory=8&platform=PC&downlink=10&effective_type=4g&round_trip_time=50&webid=7405403544101193253&msToken=rEpEllX6eVlseu50ClnxP9NbX-q7bb6sYNheTM1Dgyd4Xk5xsIIoJjNxl_d9BWHSlfeeHI2Rpxrrb2qv4hAU3ShEWnoek6d-ySl9SbIk8DQ4awPOGfdbeqFC9YEVlxykbeo9pGrJ0vkMshUQszxoITJeO1lpv8CzFYpfIa5-Aqh5tzL9p8cuY5DM";
        let data = "bff_type=2&is_h5=1&origin_type=638301&promotion_id=3704239957154652788";
        let user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36";

        match engine.call_function("getABogus", &[url, data, user_agent]) {
            Ok(res) => match res {
                JsReturn::Text(t) => println!("Result: {}", t),
                JsReturn::Number(n) => println!("Result: {}", n),
                JsReturn::Bool(b) => println!("Result: {}", b),
                JsReturn::Json(v) => println!("Result: {}", v),
                JsReturn::Bytes(bytes) => println!(
                    "Result bytes ({}): {:02x?}",
                    bytes.len(),
                    &bytes[..bytes.len().min(64)]
                ),
            },
            Err(e) => eprintln!("Call failed: {e}"),
        }

        // Example: register a Rust function into imports for WASM/JS to call
        engine.register_import_fn("env", "host_log", |args| {
            println!("host_log called with: {:?}", args);
            Ok(JsReturn::Number(0.0))
        })?;
        // Call it from JS to demonstrate the plumbing (your WASM can import env.host_log and call it)
        engine.exec_script(
            "globalThis.__host_imports.env.host_log('hello', 123, true);",
            None,
        )?;
        // Load the wasm-bindgen output .wasm and run it inside V8 without ESM glue
        let wasm = fs::read("encrypt_wasm_bg.wasm")?;

        // Provide minimal imports expected by wasm-bindgen when targeting browsers
        // Imports: module "wbg" with functions __wbindgen_throw and __wbindgen_init_externref_table
        // We bind to the instance via globalThis.wasm_inst after instantiation.
        let imports_js = r#"({
            wbg: {
                    __wbindgen_throw: (ptr, len) => {
                        const mem = new Uint8Array(globalThis.wasm_inst.exports.memory.buffer);
                        const bytes = mem.subarray((ptr>>>0), (ptr>>>0) + (len>>>0));
                        let pct = '';
                        for (let i=0;i<bytes.length;i++) pct += '%' + bytes[i].toString(16).padStart(2,'0');
                        const msg = decodeURIComponent(pct);
                        throw new Error(msg);
                    },
                __wbindgen_init_externref_table: () => {
                    // wasm-bindgen expects the externref table to be grown and seeded
                    const table = globalThis.wasm_inst.exports.__wbindgen_export_0;
                    const offset = table.grow(4);
                    table.set(0, undefined);
                    table.set(offset + 0, undefined);
                    table.set(offset + 1, null);
                    table.set(offset + 2, true);
                    table.set(offset + 3, false);
                }
            }
        })"#;

        // Instantiate the module into globalThis.wasm_inst and wasm_inst_exports
        engine.instantiate_wasm_global("wasm_inst", &wasm, Some(imports_js))?;

        // Call the start function exported by wasm-bindgen to run any initialization
        let _ = engine.call_wasm_export::<&str>("wasm_inst", "__wbindgen_start", &[]);

        // Inject small JS helpers that mirror wasm-bindgen wrappers for string I/O
        engine.exec_script(r#"
            (function(){
                    function encodeUTF8(str){
                        const enc = encodeURIComponent(String(str));
                        const parts = enc.match(/%[0-9A-Fa-f]{2}|[^%]/g) || [];
                        const out = new Uint8Array(parts.length);
                        for (let i=0;i<parts.length;i++){
                            const p = parts[i];
                            out[i] = p.length === 3 && p[0] === '%' ? parseInt(p.slice(1),16) : p.charCodeAt(0);
                        }
                        return out;
                    }
                    function decodeUTF8(bytes){
                        let pct = '';
                        for (let i=0;i<bytes.length;i++) pct += '%' + bytes[i].toString(16).padStart(2,'0');
                        return decodeURIComponent(pct);
                    }
                function u8() { return new Uint8Array(globalThis.wasm_inst.exports.memory.buffer); }

                globalThis.encrypt = function(data) {
                    const wasm = globalThis.wasm_inst.exports;
                        const input = encodeUTF8(String(data));
                    const ptr = wasm.__wbindgen_malloc(input.length, 1) >>> 0;
                    u8().subarray(ptr, ptr + input.length).set(input);
                    const ret = wasm.encrypt(ptr, input.length);
                        const out = decodeUTF8(u8().subarray(ret[0] >>> 0, (ret[0] >>> 0) + (ret[1] >>> 0)));
                    wasm.__wbindgen_free(ret[0], ret[1], 1);
                    return out;
                };

                globalThis.decrypt = function(data) {
                    const wasm = globalThis.wasm_inst.exports;
                        const input = encodeUTF8(String(data));
                    const ptr = wasm.__wbindgen_malloc(input.length, 1) >>> 0;
                    u8().subarray(ptr, ptr + input.length).set(input);
                    const ret = wasm.decrypt(ptr, input.length);
                        const out = decodeUTF8(u8().subarray(ret[0] >>> 0, (ret[0] >>> 0) + (ret[1] >>> 0)));
                    wasm.__wbindgen_free(ret[0], ret[1], 1);
                    return out;
                };
            })();
        "#, None)?;

        let res = match engine.call_function("encrypt", &["abc"])? {
            JsReturn::Text(s) => {
                println!("encrypt(abc) = {}", s);
                s
            }
            other => panic!("Unexpected return: {:?}", other),
        };
        match engine.call_function("decrypt", &[res])? {
            JsReturn::Text(s) => println!("decrypt(abc) = {}", s),
            other => panic!("Unexpected return: {:?}", other),
        }
        Ok(())
    }
}
