use wind_event::def::Directory;

type Source = (&'static str, fn() -> Directory);

const SOURCES: [Source; 8] = [
    ("usecase", wind_core::usecase::directory),
    ("agent::events", wind_core::agent::events::directory),
    ("conversation", wind_core::conversation::directory),
    ("llm", wind_core::llm::directory),
    ("mod::events", wind_mod::events::directory),
    ("mcp::events", wind_mcp::events::directory),
    ("script::events", wind_script::events::directory),
    ("event::events", wind_event::events::directory),
];

pub const ORDER: [&str; 8] = [
    "usecase",
    "agent::events",
    "conversation",
    "llm",
    "mod::events",
    "mcp::events",
    "script::events",
    "event::events",
];

pub fn merged() -> Directory {
    debug_assert_eq!(
        SOURCES.map(|(name, _)| name),
        ORDER,
        "The order of merging must be consistent with ORDER"
    );
    SOURCES
        .into_iter()
        .fold(Directory::default(), |merged, (_, source)| {
            merged.merge(source())
        })
}
