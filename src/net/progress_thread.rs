use std::sync::mpsc::Receiver;
use progress::Progress;

pub trait ProgressThread {
    fn progress_rx(&self) -> &Receiver<Progress>;
}