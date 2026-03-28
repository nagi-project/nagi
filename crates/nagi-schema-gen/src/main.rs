fn main() {
    nagi_core::interface::schema::generate_schemas_to_docs().expect("failed to generate schemas");
    eprintln!("schemas written to docs/schemas/");
}
