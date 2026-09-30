//! Tests for handler route validation and introspection.

use rustclamp_core::{ContributionTarget, ModuleId};
use rustclamp_worker::{HandlerBuildError, HandlerDeclaration, HandlerTarget};

const MAIL: ModuleId = ModuleId::new("test.module.mail");
const ORDERS: ModuleId = ModuleId::new("test.module.orders");

fn declaration(name: &str, version: u32) -> HandlerDeclaration {
    HandlerDeclaration::new(name, version, |_| async { Ok(()) })
}

#[test]
fn registry_reports_its_routes() {
    let registry = HandlerTarget
        .build(&[
            (ORDERS, declaration("order.created", 2)),
            (MAIL, declaration("mail.send", 1)),
            (ORDERS, declaration("order.created", 1)),
        ])
        .unwrap();

    assert!(registry.contains("mail.send", 1));
    assert!(registry.contains("order.created", 2));
    assert!(!registry.contains("mail.send", 2));
    assert!(!registry.contains("mail", 1));
    assert_eq!(
        registry.routes().collect::<Vec<_>>(),
        [("mail.send", 1), ("order.created", 1), ("order.created", 2)]
    );
}

#[test]
fn duplicate_routes_fail_at_build() {
    let error = HandlerTarget
        .build(&[
            (MAIL, declaration("mail.send", 1)),
            (ORDERS, declaration("mail.send", 1)),
        ])
        .err()
        .unwrap();
    assert!(matches!(error, HandlerBuildError::Duplicate { .. }));
}
