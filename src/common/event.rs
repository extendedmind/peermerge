use automerge::Automerge;

#[derive(Clone, Debug)]
pub enum StateEvent {
    DocumentLoaded(Automerge),
}

#[derive(Clone, Debug)]
pub enum SynchronizeEvent {
    NewPeersAdvertised(usize),
    DocumentCreated(),
}
