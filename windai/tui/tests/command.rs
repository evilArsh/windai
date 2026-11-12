use wind_tui::command::{COMMANDS, Command, candidates, parse};

#[test]
fn parses_all_eight_commands() {
    let cases = [
        ("/new", Command::New),
        ("/resume", Command::Resume),
        ("/providers", Command::Providers),
        ("/models", Command::Models),
        ("/config", Command::Config),
        ("/cancel", Command::Cancel),
        ("/help", Command::Help),
        ("/quit", Command::Quit),
    ];
    for (input, expected) in cases {
        assert_eq!(parse(input), Ok(expected), "parse {input}");
    }
}

#[test]
fn unknown_commands_report_an_error() {
    assert_eq!(parse("/nope"), Err("unknown command".to_owned()));
    assert_eq!(parse("/"), Err("unknown command".to_owned()));
    assert_eq!(parse("/newer"), Err("unknown command".to_owned()));
    assert_eq!(parse("/new/"), Err("unknown command".to_owned()));
}

#[test]
fn command_names_are_case_sensitive() {
    assert_eq!(parse("/NEW"), Err("unknown command".to_owned()));
    assert_eq!(parse("/New"), Err("unknown command".to_owned()));
    assert_eq!(parse("/Quit"), Err("unknown command".to_owned()));
}

#[test]
fn surrounding_whitespace_is_ignored() {
    assert_eq!(parse("  /new  "), Ok(Command::New));
    assert_eq!(parse("\t/quit\n"), Ok(Command::Quit));
    assert_eq!(parse(" /models "), Ok(Command::Models));
}

#[test]
fn arguments_after_the_command_name_are_ignored() {
    assert_eq!(parse("/new hello world"), Ok(Command::New));
    assert_eq!(parse("/resume 42"), Ok(Command::Resume));
    assert_eq!(parse("/cancel\tnow"), Ok(Command::Cancel));
}

#[test]
fn non_commands_are_not_parsed_as_commands() {
    assert!(parse("hello").is_err());
    assert!(parse("").is_err());
    assert!(parse("   ").is_err());
    assert!(parse("new").is_err());
    assert!(parse(" / ").is_err());
}

#[test]
fn the_command_table_lists_commands_in_completion_order() {
    let names: Vec<&str> = COMMANDS.iter().map(|spec| spec.name).collect();
    assert_eq!(
        names,
        [
            "/new",
            "/resume",
            "/providers",
            "/models",
            "/config",
            "/cancel",
            "/help",
            "/quit"
        ]
    );
    assert!(COMMANDS.iter().all(|spec| !spec.description.is_empty()));
}

#[test]
fn an_empty_prefix_lists_every_command() {
    let names: Vec<&str> = candidates("").iter().map(|spec| spec.name).collect();
    assert_eq!(
        names,
        COMMANDS.iter().map(|spec| spec.name).collect::<Vec<_>>()
    );
}

#[test]
fn completion_matches_prefixes_case_insensitively_in_table_order() {
    let names = |prefix: &str| -> Vec<&'static str> {
        candidates(prefix)
            .into_iter()
            .map(|spec| spec.name)
            .collect()
    };
    assert_eq!(names("ne"), ["/new"]);
    assert_eq!(names("NE"), ["/new"]);
    assert_eq!(names("re"), ["/resume"]);
    assert_eq!(names("mo"), ["/models"]);
    assert_eq!(names("c"), ["/config", "/cancel"]);
    assert_eq!(names("q"), ["/quit"]);
    assert_eq!(names("m"), ["/models"]);
    assert!(names("zz").is_empty());
}

#[test]
fn completion_accepts_a_leading_slash_in_the_prefix() {
    let names: Vec<&str> = candidates("/ne").iter().map(|spec| spec.name).collect();
    assert_eq!(names, ["/new"]);
}
