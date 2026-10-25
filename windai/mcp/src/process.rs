use super::params::StdioParams;
use std::process::Stdio;
use tokio::process::Command;

pub fn build_command(params: &StdioParams) -> Command {
    let mut command = Command::new(&params.command);
    command.args(&params.args);
    if let Some(env) = &params.env {
        for (key, value) in env {
            command.env(key, value);
        }
    }
    if let Some(cwd) = &params.cwd {
        command.current_dir(cwd);
    }
    command.stderr(Stdio::null());
    command.kill_on_drop(true);
    detach(command.as_std_mut());
    command
}

#[cfg(unix)]
pub(crate) fn detach(command: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

#[cfg(not(unix))]
pub(crate) fn detach(_command: &mut std::process::Command) {}
