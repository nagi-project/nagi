fn main() {
    nagi_core::schema::generate_schemas_to_docs().expect("failed to generate schemas");
    eprintln!("schemas written to docs/schemas/");
}
