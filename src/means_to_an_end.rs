use std::{collections::BTreeMap, ops::Bound::Included};

use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn handle_client(mut socket: TcpStream) -> io::Result<()> {
    let mut assets: BTreeMap<i32, i32> = BTreeMap::new();

    let mut buffer = [0u8; 9];

    loop {
        socket.read_exact(&mut buffer).await?;

        let l = i32::from_be_bytes(buffer[1..5].try_into().unwrap());
        let r = i32::from_be_bytes(buffer[5..9].try_into().unwrap());

        match buffer[0] {
            b'I' => {
                assets.insert(l, r);
            }

            b'Q' => {
                if l > r {
                    socket.write_all(&0i32.to_be_bytes()).await?;
                } else {
                    let mut sum: i64 = 0;
                    let mut counter: i64 = 0;

                    for (_, val) in assets.range((Included(l), Included(r))) {
                        sum += *val as i64;
                        counter += 1;
                    }

                    if counter == 0 {
                        socket.write_all(&0i32.to_be_bytes()).await?;
                    } else {
                        sum /= counter;
                        socket.write_all(&(sum as i32).to_be_bytes()).await?;
                    }
                }
            }

            _ => return Ok(()),
        }
    }
}

pub async fn solve() -> io::Result<()> {
    let listener = TcpListener::bind("[::]:6969").await?;

    loop {
        match listener.accept().await {
            Ok((socket, ..)) => {
                tokio::spawn(async move {
                    let _ = handle_client(socket).await;
                });
            }
            Err(_) => {}
        }
    }
}
