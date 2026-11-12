#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    New,
    Resume,
    Providers,
    Models,
    Config,
    Cancel,
    Help,
    Quit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: &'static str,
    pub command: Command,
    pub description: &'static str,
}

pub const COMMANDS: [CommandSpec; 8] = [
    CommandSpec {
        name: "/new",
        command: Command::New,
        description: "start a new session",
    },
    CommandSpec {
        name: "/resume",
        command: Command::Resume,
        description: "resume an existing topic",
    },
    CommandSpec {
        name: "/providers",
        command: Command::Providers,
        description: "choose a provider",
    },
    CommandSpec {
        name: "/models",
        command: Command::Models,
        description: "choose a model",
    },
    CommandSpec {
        name: "/config",
        command: Command::Config,
        description: "open the configuration overlay",
    },
    CommandSpec {
        name: "/cancel",
        command: Command::Cancel,
        description: "cancel the current topic",
    },
    CommandSpec {
        name: "/help",
        command: Command::Help,
        description: "show commands and keys",
    },
    CommandSpec {
        name: "/quit",
        command: Command::Quit,
        description: "quit the application",
    },
];

pub fn parse(input: &str) -> Result<Command, String> {
    let trimmed = input.trim();
    let Some(rest) = trimmed.strip_prefix('/') else {
        return Err("not a command".to_owned());
    };
    let name = rest.split_whitespace().next().unwrap_or_default();
    COMMANDS
        .iter()
        .find(|spec| spec.name.strip_prefix('/') == Some(name))
        .map(|spec| spec.command)
        .ok_or_else(|| "unknown command".to_owned())
}

pub fn candidates(prefix: &str) -> Vec<&'static CommandSpec> {
    let needle = prefix.strip_prefix('/').unwrap_or(prefix).to_lowercase();
    COMMANDS
        .iter()
        .filter(|spec| {
            spec.name
                .strip_prefix('/')
                .unwrap_or(spec.name)
                .to_lowercase()
                .starts_with(&needle)
        })
        .collect()
}
