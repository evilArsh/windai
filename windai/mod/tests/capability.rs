//! ctx 属性集的两级裁剪

use std::collections::BTreeSet;
use wind_event::host::CallMode;
use wind_mod::capability::{Capability, Prop, available};

#[test]
fn an_undeclared_capability_is_absent() {
    let set = available(&[], CallMode::Observe);
    assert!(!set.has(Prop::Http), "undeclared means absent");
    assert!(set.has(Prop::Log), "logging needs no declaration");
    assert!(set.has(Prop::Emit), "emitting events needs no declaration");
    assert!(set.has(Prop::Notify), "notifications need no declaration");
}

#[test]
fn a_declared_capability_is_present_in_every_io_mode() {
    for mode in [CallMode::Observe, CallMode::Request] {
        let set = available(&[Capability::Http], mode);
        assert!(set.has(Prop::Http), "should be present under {mode:?}");
    }
}

#[test]
fn interception_never_gets_a_io_capability() {
    let declared = [
        Capability::Http,
        Capability::Agent,
        Capability::Fs,
        Capability::State,
        Capability::Storage,
    ];
    let set = available(&declared, CallMode::Intercept);
    assert!(
        !set.has(Prop::Http),
        "an intercept handler blocks the system, so network egress would always time out"
    );
    assert!(!set.has(Prop::Agent));
    assert!(!set.has(Prop::Fs));
    assert!(
        set.has(Prop::State),
        "state is an in-memory operation and is not trimmed"
    );
    assert!(set.has(Prop::Storage));
    assert!(set.has(Prop::Log));
    assert!(
        set.has(Prop::Emit),
        "emitting events needs no declaration and is not trimmed"
    );
    assert!(
        set.has(Prop::Notify),
        "notifications need no declaration and are not trimmed"
    );
}

#[test]
fn the_set_lists_exactly_what_it_holds() {
    let set = available(&[Capability::State, Capability::Fs], CallMode::Request);
    let expected: BTreeSet<Prop> = [Prop::State, Prop::Fs, Prop::Emit, Prop::Notify, Prop::Log]
        .into_iter()
        .collect();
    assert_eq!(set.iter().collect::<BTreeSet<_>>(), expected);
}

#[test]
fn every_capability_maps_to_exactly_one_property() {
    for capability in [
        Capability::State,
        Capability::Storage,
        Capability::Http,
        Capability::Agent,
        Capability::Fs,
    ] {
        let set = available(&[capability], CallMode::Request);
        let expected: BTreeSet<Prop> = [capability.into(), Prop::Emit, Prop::Notify, Prop::Log]
            .into_iter()
            .collect();
        assert_eq!(
            set.iter().collect::<BTreeSet<_>>(),
            expected,
            "{capability:?}"
        );
    }
}
