// use rmcp::{
//     ServiceExt,
//     transport::{ConfigureCommandExt, TokioChildProcess},
// };
// use tokio::process::Command;

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let client = ()
//         .serve(TokioChildProcess::new(Command::new("npx").configure(
//             |cmd| {
//                 cmd.arg("-y").arg("@modelcontextprotocol/server-everything");
//             },
//         ))?)
//         .await?;
//     Ok(())
// }
