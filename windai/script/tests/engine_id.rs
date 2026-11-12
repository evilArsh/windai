#[test]
fn the_engine_id_names_every_input_that_changes_bytecode() {
    let id = wind_script::engine_id();
    let endian = if cfg!(target_endian = "little") {
        "le"
    } else {
        "be"
    };
    assert_eq!(
        id,
        format!(
            "rquickjs-sys={};arch={};ptr={};endian={endian}",
            env!("WIND_SCRIPT_ENGINE_VERSION"),
            std::env::consts::ARCH,
            usize::BITS
        )
    );
}
