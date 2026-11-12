use std::path::Path;
use wind_wire::AppDirs;
#[test]
fn bytecode_root_sits_beside_the_mods_root() {
    let root = Path::new("test-root");
    let dirs = AppDirs::under(root);
    assert_eq!(dirs.bytecode_root(), root.join("bytecode"));
    assert_eq!(dirs.bytecode_root().parent(), Some(dirs.root()));
    assert_ne!(dirs.bytecode_root(), dirs.mods_root());
}
