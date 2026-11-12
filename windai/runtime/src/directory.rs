use wind_core::{agent::events as agent, llm::events as llm, script};
use wind_event::Directory;
use wind_mcp::events as mcp;
use wind_mod::events as mods;

type Source = (&'static str, fn() -> Directory);
const SOURCES: [Source; 6] = [
    ("event", wind_event::directory),
    ("agent", agent::directory),
    ("llm", llm::directory),
    ("mod", mods::directory),
    ("mcp", mcp::directory),
    ("script", script::directory),
];

pub(crate) fn merged() -> Directory {
    SOURCES
        .into_iter()
        .fold(Directory::default(), |directory, (_, source)| {
            directory.merge(source())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn includes_all_event_sources_without_collisions() {
        let sources = SOURCES.map(|(_, source)| source());
        let total: usize = sources.iter().map(Directory::len).sum();
        let expected: BTreeSet<_> = sources.iter().flat_map(Directory::names).collect();
        assert_eq!(expected.len(), total);
        let merged = merged();
        assert_eq!(merged.names().collect::<BTreeSet<_>>(), expected);
        assert_eq!(merged.len(), total);
        assert_eq!(super::merged().names().collect::<BTreeSet<_>>(), expected);
    }
}
