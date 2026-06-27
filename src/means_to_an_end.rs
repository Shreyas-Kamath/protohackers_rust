use std::{collections::BTreeMap, net::SocketAddr, ops::Bound::Included};

use tokio::{io::{self, AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

async fn handle_req(mut socket: TcpStream, _addr: SocketAddr) -> io::Result<()> {
    let mut price_map: BTreeMap<i32, i32> = BTreeMap::new();

    let mut buff = [0u8; 9];

    loop {
        let bytes = socket.read_exact(&mut buff).await?;

        if bytes == 0 {
            return Ok(());
        }

        let left = i32::from_be_bytes(buff[1..=4].try_into().unwrap());
        let right = i32::from_be_bytes(buff[5..=8].try_into().unwrap());

        match buff[0] {
            b'I' => { price_map.insert(left, right); },
            b'Q' => {
                if left > right {
                    socket.write_all(&0i32.to_be_bytes()).await?;
                }
                else {
                    let (sum, count) = price_map
                        .range((Included(left), Included(right)))
                        .fold((0i64, 0usize), |(sum, count), (_, &price)| {
                            (sum + (price as i64), count + 1)
                    });

                    if count == 0 {
                        socket.write_all(&0i32.to_be_bytes()).await?;
                    }
                    else {
                        let avg = (sum / (count as i64)) as i32;
                        socket.write_all(&avg.to_be_bytes()).await?;
                    }
                }
            }

            _ => {
                return Ok(())
            }
        }
    }
}

pub async fn solve() -> io::Result<()> {
    let acc = TcpListener::bind("[::]:6969").await?;

    loop {
        let (socket, addr) = acc.accept().await?;

        tokio::spawn(async move {
            if let Err(e) = handle_req(socket, addr).await {
                eprintln!("{}", e);
            }
        });
    }  
}