fn main() {
    if let Err(error) = wind_tui::app::run() {
        eprintln!("wind-tui: {error}");
        std::process::exit(1);
    }
}
