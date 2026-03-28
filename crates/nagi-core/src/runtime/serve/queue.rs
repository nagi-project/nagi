use std::collections::{HashSet, VecDeque};

/// FIFO queue with deduplication. An asset that is already queued will not be
/// added again until it is dequeued.
#[derive(Debug, Default)]
pub struct WorkQueue {
    queue: VecDeque<String>,
    pending: HashSet<String>,
}

impl WorkQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            pending: HashSet::new(),
        }
    }

    /// Enqueues an asset. Returns false if already queued.
    pub fn enqueue(&mut self, name: String) -> bool {
        if self.pending.contains(&name) {
            return false;
        }
        self.pending.insert(name.clone());
        self.queue.push_back(name);
        true
    }

    pub fn dequeue(&mut self) -> Option<String> {
        let name = self.queue.pop_front()?;
        self.pending.remove(&name);
        Some(name)
    }
}

#[cfg(test)]
impl WorkQueue {
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_enqueue_dequeue() {
        let mut q = WorkQueue::new();
        assert!(q.is_empty());

        assert!(q.enqueue("a".to_string()));
        assert!(q.enqueue("b".to_string()));
        assert!(!q.is_empty());

        assert_eq!(q.dequeue(), Some("a".to_string()));
        assert_eq!(q.dequeue(), Some("b".to_string()));
        assert_eq!(q.dequeue(), None);
        assert!(q.is_empty());
    }

    #[test]
    fn queue_dedup() {
        let mut q = WorkQueue::new();
        assert!(q.enqueue("a".to_string()));
        assert!(!q.enqueue("a".to_string())); // duplicate rejected
        assert_eq!(q.dequeue(), Some("a".to_string()));

        // After dequeue, can enqueue again
        assert!(q.enqueue("a".to_string()));
        assert_eq!(q.dequeue(), Some("a".to_string()));
    }
}
