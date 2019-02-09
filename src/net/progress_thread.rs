use futures::channel::mpsc::Receiver;
use crate::termio::progress::Progress;

pub trait ProgressThread {
    fn progress_rx(&mut self) -> &mut Receiver<Progress>;
}