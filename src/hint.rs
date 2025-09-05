use crate::SharedContext;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender};
use tracing::error;

pub struct FileHintService {
    /// Hint creator thread handle. Hint files are created each time a rotation happens.
    /// We do it asynchronously with a background thread to avoid slowing down write operations
    /// in case there is a rotation too much. The handle is part of the engine to allow its
    /// lifetime to be tied to the engine one.
    hint_handle: std::thread::JoinHandle<()>,
    pub sender: Sender<HintMessage>,
}

pub enum HintMessage {
    Hint(u64),
    Stop,
}

impl FileHintService {
    pub fn new<const USE_CRC: bool>(ctx: Arc<SharedContext<USE_CRC>>) -> Self {
        let (sender, receiver): (Sender<HintMessage>, Receiver<HintMessage>) =
            std::sync::mpsc::channel();

        let hint_handle = std::thread::spawn(move || {
            loop {
                match receiver.recv() {
                    Ok(msg) => match msg {
                        HintMessage::Stop => return,
                        HintMessage::Hint(file_id) => {
                            create_hint_file_for(file_id, &ctx);
                        }
                    },
                    Err(err) => {
                        error!("hint service stopped unexpectedly with error: {}", err);
                        error!("system exiting on the previous error");
                        // Terminate the program because this should not happen in normal operation
                        // TODO, Try coordinate shutdown to avoid data loss
                        std::process::exit(1);
                    }
                }
            }
        });

        Self {
            hint_handle,
            sender,
        }
    }

    pub fn notify_hint(&self, file_id: u64) {
        // TODO: Having an error here is clearly a programming error, to manage
        self.sender.send(HintMessage::Hint(file_id)).unwrap();
    }

    pub fn join(self) {
        drop(self.sender);
        self.hint_handle.join().unwrap();
    }
}

fn create_hint_file_for<const USE_CRC: bool>(p0: u64, p1: &Arc<SharedContext<USE_CRC>>) {
    todo!()
}
