fn main() -> Result<(), String> {
    let version = rustc_version::version().unwrap();
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");
    println!("cargo:rustc-env=RUSTC_VERSION={version}");
    println!("cargo:rerun-if-changed=src/proto/datafusion.proto");
    println!("cargo:rerun-if-changed=src/proto/raysql.proto");
    tonic_build::configure()
        .extern_path(".datafusion", "::datafusion_proto::protobuf")
        .compile(&["src/proto/raysql.proto"], &["src/proto"])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;
    Ok(())
}
