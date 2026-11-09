use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSpec {
    pub name: String,
    pub description: Option<String>,
    pub transport: Transport,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Transport {
    Stdio(StdioSpec),
    Streamable { url: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StdioSpec {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Fingerprint(String);

impl ServiceSpec {
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = DefaultHasher::new();
        match &self.transport {
            Transport::Stdio(stdio) => {
                0u8.hash(&mut hasher);
                stdio.command.hash(&mut hasher);
                stdio.args.hash(&mut hasher);
                stdio.env.hash(&mut hasher);
                stdio.cwd.hash(&mut hasher);
            }
            Transport::Streamable { url } => {
                1u8.hash(&mut hasher);
                url.hash(&mut hasher);
            }
        }
        Fingerprint(format!("{:016x}", hasher.finish()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::{ServiceSpec, StdioSpec, Transport};

    fn stdio(command: &str, args: &[&str], cwd: &str) -> ServiceSpec {
        ServiceSpec {
            name: "demo".to_owned(),
            description: None,
            transport: Transport::Stdio(StdioSpec {
                command: command.to_owned(),
                args: args.iter().map(|arg| (*arg).to_owned()).collect(),
                env: BTreeMap::new(),
                cwd: PathBuf::from(cwd),
            }),
        }
    }

    fn with_env(mut spec: ServiceSpec, key: &str, value: &str) -> ServiceSpec {
        let Transport::Stdio(stdio) = &mut spec.transport else {
            panic!("expected a stdio transport");
        };
        stdio.env.insert(key.to_owned(), value.to_owned());
        spec
    }

    #[test]
    fn fingerprint_ignores_field_order_and_tracks_every_field() {
        let base = stdio("npx", &["-y", "server-filesystem"], "/tmp/a");
        assert_eq!(base.fingerprint(), base.clone().fingerprint());
        let other_cwd = stdio("npx", &["-y", "server-filesystem"], "/tmp/b");
        assert_ne!(base.fingerprint(), other_cwd.fingerprint());
        let other_env = with_env(base.clone(), "TOKEN", "x");
        assert_ne!(base.fingerprint(), other_env.fingerprint());
    }
}
