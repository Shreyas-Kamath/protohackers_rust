use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn handle_client(mut socket: TcpStream) -> io::Result<()> {
    let mut buffer = [0u8; 4096];

    loop {
        let bytes_read = socket.read(&mut buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        socket.write_all(&buffer[..bytes_read]).await?;
    }
}

pub async fn solve() -> io::Result<()> {
    let listener = TcpListener::bind("[::]:6969").await?;

    loop {
        match listener.accept().await {
            Ok((socket, ..)) => {
                tokio::spawn(async move { handle_client(socket).await });
            }
            Err(_) => {}
        }
    }
}
