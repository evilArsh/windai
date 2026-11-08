use std::path::Path;

use tokio::process::Command;

/// 构造子进程命令，环境变量只使用传入的集合
pub(crate) fn command(
    program: &Path,
    args: &[String],
    cwd: &Path,
    environment: &[(String, String)],
) -> Command {
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .kill_on_drop(true);
    for (key, value) in environment {
        command.env(key, value);
    }
    #[cfg(unix)]
    command.process_group(0);
    command
}
