use std::{collections::HashMap, net::SocketAddr};

use tokio::{io::{self, AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::mpsc::{self, Receiver, Sender}};

enum ServerEvent {
    Join {
        username: String,
        sender: mpsc::Sender<String>,
    },
    Leave {
        username: String,
    },
    Chat {
        username: String,
        text: String,
    },
}

async fn server_task(mut receiver: Receiver<ServerEvent>) -> io::Result<()> {
    let mut users: HashMap<String, mpsc::Sender<String>> = HashMap::new();

    while let Some(event) = receiver.recv().await {
        match event {
            ServerEvent::Join { username, sender } => {
                if users.contains_key(&username) { continue; }

                let greeting = "* The room contains: ".to_string() + &users.keys().map(|s| s.as_str()).collect::<Vec<_>>().join(", ") + "\n";
                let _ = sender.send(greeting).await;

                for sender in users.values() {
                    let _ = sender.send(format!("* {username} has entered the room\n")).await;
                }                

                users.insert(username, sender);
            },

            ServerEvent::Chat { username, text } => {
                for (user, sender) in &users {
                    if *user != username {
                        let _ = sender.send(format!("[{username}] {text}\n")).await;
                    }
                }
            },

            ServerEvent::Leave { username } => {
                users.remove(&username);
                for sender in users.values() {
                    let _ = sender.send(format!("* {username} has left the room\n")).await;
                }
            }
        }
    }

    Ok(())
}

async fn read_username(socket: &mut TcpStream) -> io::Result<Option<String>> {
    socket.write_all(b"Welcome to budgetchat! What shall I call you?\n").await?;

    let mut buff: Vec<u8> = Vec::new();
    let mut temp = [0u8; 1024];

    loop {
        let bytes = socket.read(&mut temp).await?;

        if bytes == 0 {
            return Ok(None);
        }

        buff.extend_from_slice(&temp[..bytes]);

        if let Some(pos) = buff.iter().position(|&b| b == b'\n') {
            let name_str = std::str::from_utf8(&buff[..pos]).unwrap().to_owned();
            if name_str.is_empty() || !name_str.chars().all(|c| c.is_ascii_alphanumeric()) {
                return Ok(None)
            }
            
            buff.drain(..=pos);
            return Ok(Some(name_str.to_string()));
        }
    }
}

async fn client_task(mut socket: TcpStream, _addr: SocketAddr, tx: Sender<ServerEvent>) -> io::Result<()> {
    let username = match read_username(&mut socket).await? {
        Some(u) => u,
        None => return Ok(()),
    };
    let (client_tx, mut client_rx) = mpsc::channel(32);
    let _ = tx.send(ServerEvent::Join { username: username.clone(), sender: client_tx.clone() }).await;

    let mut buff: Vec<u8> = Vec::new();
    let mut temp = [0u8; 2048];

    loop {
        tokio::select! {
            // Server wants to send us something
            Some(msg) = client_rx.recv() => {
                socket.write_all(msg.as_bytes()).await?;
            }

            // Client sent us something
            result = socket.read(&mut temp) => {
                let bytes = result?;

                if bytes == 0 {
                    let _ = tx.send(ServerEvent::Leave { username }).await;
                    return Ok(());
                }

                buff.extend_from_slice(&temp[..bytes]);

                while let Some(pos) = buff.iter().position(|&b| b == b'\n') {
                    let message = std::str::from_utf8(&buff[..pos]).unwrap().to_owned();
                    buff.drain(..=pos);
                    let _ = tx.send(ServerEvent::Chat { username: username.clone(), text: message.to_string() }).await;
                }
            }
        }
    }
}

pub async fn solve() -> io::Result<()> {

    let acc = TcpListener::bind("[::]:6969").await?;
    
    let (tx, rx) = mpsc::channel(32);

    tokio::spawn(async move {
        if let Err(e) = server_task(rx).await {
            eprintln!("{}", e);
        }
    });

    loop {
        let (socket, addr) = acc.accept().await?;
        let t = tx.clone();

        tokio::spawn(async move {
            if let Err(e) = client_task(socket, addr, t).await {
                eprintln!("{}", e);
            }
        });
    }
}