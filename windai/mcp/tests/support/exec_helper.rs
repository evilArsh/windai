use std::io::Write;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("echo") => {
            println!("{}", args.get(1).map(String::as_str).unwrap_or_default());
        }
        Some("stderr") => {
            eprintln!("{}", args.get(1).map(String::as_str).unwrap_or_default());
        }
        Some("sleep") => {
            let millis = args
                .get(1)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            std::thread::sleep(Duration::from_millis(millis));
        }
        Some("large") => {
            let bytes: usize = args
                .get(1)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let chunk = vec![b'x'; 8192];
            let mut stdout = std::io::stdout().lock();
            let mut written = 0;
            while written < bytes {
                let take = chunk.len().min(bytes - written);
                stdout
                    .write_all(&chunk[..take])
                    .expect("the output must accept bytes");
                written += take;
            }
            stdout.flush().expect("the output must flush");
        }
        other => {
            eprintln!("unknown command: {other:?}");
            std::process::exit(2);
        }
    }
}
