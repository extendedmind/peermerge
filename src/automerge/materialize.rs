use automerge::{Automerge, Prop, Value, ROOT};

/// Materialize an Automerge
pub fn materialize_root_property<P: Into<Prop>>(doc: &Automerge, prop: P) -> Option<Value> {
    doc.get(ROOT, prop).unwrap().map(|result| result.0)
}
