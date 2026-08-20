use std::path::Path;

fn main() {
    // Compile Gluten's patched Substrait protos (they carry Gluten-specific
    // extensions such as ReadRel.LocalFiles.FileOrFiles.partition_columns) so
    // the generated types stay in sync with what the JVM side serializes.
    let proto_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../gluten-substrait/src/main/resources/substrait/proto")
        .canonicalize()
        .expect("Gluten substrait proto directory not found");

    if std::env::var("PROTOC").is_err() {
        std::env::set_var(
            "PROTOC",
            protoc_bin_vendored::protoc_bin_path().expect("no vendored protoc for this platform"),
        );
    }

    println!(
        "cargo:rerun-if-changed={}",
        proto_root.join("substrait").display()
    );

    prost_build::Config::new()
        .compile_protos(
            &[proto_root.join("substrait/plan.proto")],
            &[proto_root.as_path()],
        )
        .expect("failed to compile substrait protos");
}
