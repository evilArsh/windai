//! 把八份目录合并成一份。漏并与同名相撞都要能在总数上暴露

#[test]
fn the_merged_directory_holds_every_subsystem() {
    let parts = [
        wind_core::usecase::directory(),
        wind_core::agent::events::directory(),
        wind_core::conversation::directory(),
        wind_core::llm::directory(),
        wind_mod::events::directory(),
        wind_mcp::events::directory(),
        wind_script::events::directory(),
        wind_event::events::directory(),
    ];
    let total: usize = parts.iter().map(|part| part.len()).sum();
    assert_eq!(
        wind_runtime::directory::merged().len(),
        total,
        "a merge that drops same-named entries would shrink the total -- this also detects name collisions"
    );
}

#[test]
fn the_chain_and_mcp_events_are_reachable_from_one_place() {
    // mcp 的目录曾经漏并，llm 的目录曾经不存在，两者各验证一次
    let directory = wind_runtime::directory::merged();
    assert!(
        directory.get("core/llm.request").is_some(),
        "the six chain events must be subscribable"
    );
    assert!(
        directory.get("core/mcp.tool.call").is_some(),
        "the mcp directory must not be omitted again"
    );
}

#[test]
fn the_merge_order_is_explicit_and_documented() {
    // 先并者胜是 `Directory::merge` 的语义，因此这个次序是契约
    assert_eq!(
        wind_runtime::directory::ORDER,
        [
            "usecase",
            "agent::events",
            "conversation",
            "llm",
            "mod::events",
            "mcp::events",
            "script::events",
            "event::events",
        ]
    );
}
