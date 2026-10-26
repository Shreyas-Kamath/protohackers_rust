use std::{
    collections::{
        HashMap,
        hash_map::Entry::{Occupied, Vacant},
    },
    sync::Arc,
};

use itertools::Itertools;
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
};

enum ServerEvent {
    NewUser(BufReader<TcpStream>, String),
    UserMessage(String, String),
    UserDisconnected(String),
}

enum UserEvent {
    Message(Arc<String>),
}

fn valid_name(raw_name: &str) -> bool {
    !raw_name.is_empty() && raw_name.chars().all(|c| char::is_ascii_alphanumeric(&c))
}

async fn ask_name(socket: TcpStream, tx: mpsc::Sender<ServerEvent>) -> io::Result<()> {
    let mut buffer: Vec<u8> = vec![];
    let mut stream = BufReader::new(socket);

    stream
        .write_all(b"Welcome to budgetchat! What shall I call you?\n")
        .await?;

    let bytes_read = stream.read_until(b'\n', &mut buffer).await?;
    if bytes_read == 0 {
        return Ok(());
    }

    match str::from_utf8(&buffer[0..bytes_read - 1]) {
        Ok(raw_name) => {
            if valid_name(raw_name) {
                let _ = tx
                    .send(ServerEvent::NewUser(stream, String::from(raw_name)))
                    .await;
            }

            Ok(())
        }
        Err(_) => Ok(()),
    }
}

async fn listen(tx: mpsc::Sender<ServerEvent>) -> io::Result<()> {
    let listener = TcpListener::bind("[::]:6969").await?;

    loop {
        if let Ok((socket, ..)) = listener.accept().await {
            let tx_1 = tx.clone();

            tokio::spawn(async move {
                let _ = ask_name(socket, tx_1).await;
            });
        }
    }
}

async fn read_loop(
    name: String,
    mut reader: BufReader<OwnedReadHalf>,
    server_tx: mpsc::Sender<ServerEvent>,
) -> io::Result<()> {
    let mut buffer: Vec<u8> = vec![];

    loop {
        let bytes_read = reader.read_until(b'\n', &mut buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        let message = str::from_utf8(&buffer[0..bytes_read]).unwrap().to_string();
        let name1 = name.clone();
        let _ = server_tx
            .send(ServerEvent::UserMessage(message, name1))
            .await;

        buffer.clear();
    }
}

async fn write_loop(
    mut writer: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<UserEvent>,
) -> io::Result<()> {
    loop {
        while let Some(message) = receiver.recv().await {
            match message {
                UserEvent::Message(str) => {
                    writer.write_all(str.as_bytes()).await?;
                }
            }
        }
    }
}

async fn run_client(
    name: String,
    stream: BufReader<TcpStream>,
    rx: mpsc::Receiver<UserEvent>,
    server_tx: mpsc::Sender<ServerEvent>,
) -> io::Result<()> {
    let socket = stream.into_inner();
    let (reader, writer) = socket.into_split();
    let readstream = BufReader::new(reader);

    let name1 = name.clone();
    let server_tx1 = server_tx.clone();

    let mut readloop = tokio::spawn(async move {
        let _ = read_loop(name1, readstream, server_tx1).await;
    });

    let mut writeloop = tokio::spawn(async move {
        let _ = write_loop(writer, rx).await;
    });

    tokio::select! {
        _ = &mut readloop => {
            writeloop.abort();
        }

        _ = &mut writeloop => {
            readloop.abort();
        }
    }

    let _ = server_tx.try_send(ServerEvent::UserDisconnected(name));

    Ok(())
}

async fn server() -> io::Result<()> {
    let (server_sender, mut server_receiver) = mpsc::channel::<ServerEvent>(32);
    let mut users: HashMap<String, mpsc::Sender<UserEvent>> = HashMap::new();

    let sender_1 = server_sender.clone();

    tokio::spawn(async move {
        let _ = listen(sender_1).await;
    });

    loop {
        while let Some(event) = server_receiver.recv().await {
            match event {
                ServerEvent::NewUser(stream, name) => {
                    let room_contains = Arc::new(format!(
                        "* The room contains: {}\n",
                        users.keys().join(", ")
                    ));

                    match users.entry(name.clone()) {
                        Occupied(_) => {}
                        Vacant(e) => {
                            let (user_tx, user_rx) = mpsc::channel::<UserEvent>(32);
                            let user_tx1 = user_tx.clone();

                            e.insert_entry(user_tx);

                            let name1 = name.clone();

                            let server_tx = server_sender.clone();
                            tokio::spawn(async move {
                                let _ = run_client(name1, stream, user_rx, server_tx).await;
                            });

                            let _ = user_tx1.try_send(UserEvent::Message(room_contains));

                            let user_joined =
                                Arc::new(format!("* {} has entered the room.\n", name));

                            for (user, sender) in &users {
                                if *user == name {
                                    continue;
                                }
                                let _ = sender.try_send(UserEvent::Message(user_joined.clone()));
                            }
                        }
                    }
                }

                ServerEvent::UserDisconnected(dced) => {
                    users.remove(&dced);

                    let user_left = Arc::new(format!("* {} has left the room\n", dced));

                    for sender in users.values() {
                        let _ = sender.try_send(UserEvent::Message(user_left.clone()));
                    }
                }

                ServerEvent::UserMessage(message, from) => {
                    let user_message = Arc::new(format!("[{}] {}", from, message));

                    for (user, sender) in &users {
                        if *user != from {
                            let _ = sender.try_send(UserEvent::Message(user_message.clone()));
                        }
                    }
                }
            }
        }
    }
}

pub async fn solve() -> io::Result<()> {
    server().await
}
