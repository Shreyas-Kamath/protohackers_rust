use std::net::SocketAddr;

use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn handle(mut socket: TcpStream, _client: SocketAddr) -> io::Result<()> {
    let mut buff = [0u8; 4096];
    println!("{_client} inside handle");

    loop {
        let bytes = socket.read(&mut buff).await?;
        if bytes == 0 { return Ok(()); }

        socket.write_all(&buff[..bytes]).await?;
    }
}

pub async fn solve() -> io::Result<()> {
    let acc = TcpListener::bind("[::]:6969").await?;

    loop {
        let (socket, addr) = acc.accept().await?;
        println!("{addr} connected");

        tokio::spawn(async move {
            if let Err(e) = handle(socket, addr).await {
                eprintln!("{}", e);
            }
        });
    }
}
