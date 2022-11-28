use automerge::{
    transaction::{CommitOptions, Transactable},
    Automerge, AutomergeError, Prop, ScalarValue, ROOT,
};

/// Convenience method to initialize an Automerge document with root scalars
pub fn init_doc_with_root_scalars<P: Into<Prop>, V: Into<ScalarValue>>(
    root_props: Vec<(P, V)>,
) -> Automerge {
    let mut doc = Automerge::new();
    doc.transact_with::<_, _, AutomergeError, _>(
        |_| CommitOptions::default().with_message("init".to_owned()),
        |tx| {
            for root_prop in root_props {
                tx.put(ROOT, root_prop.0, root_prop.1).unwrap();
            }
            Ok(())
        },
    )
    .unwrap();
    doc
}
