use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use tokio::{io::{self, AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

#[derive(Debug, Deserialize)]
struct Request {
    method: String,
    number: f64
}

#[derive(Debug, Serialize)]
struct Response {
    method: &'static str,
    prime: bool
}

fn is_prime(number: f64) -> bool {
    if number.fract() != 0.0 {
        return false;
    }

    let n = number as i64;

    if n < 2 {
        return false;
    }

    if n == 2 || n == 3 {
        return true;
    }

    if n % 2 == 0 || n % 3 == 0 {
        return false;
    }

    let mut i = 5;

    while i * i <= n {
        if n % i == 0 || n % (i + 2) == 0 {
            return false;
        }
        i += 6;
    }
    true
}

pub async fn handle_req(mut socket: TcpStream, _addr: SocketAddr) -> io::Result<()> {
    let mut buff: Vec<u8> = Vec::new();
    let mut temp = [0u8; 4096];

    loop {
        let bytes = socket.read(&mut temp).await?;
        if bytes == 0 { 
            return Ok(());
        }

        buff.extend_from_slice(&temp[..bytes]);

        while let Some(pos) = buff.iter().position(|&b| b == b'\n') {
            let req: Result<Request, _> = serde_json::from_slice(&buff[..pos]);

            if let Ok(res) = req {
                if res.method != "isPrime" {
                    socket.write_all(b"bye").await?;
                    return Ok(());
                }

                let response = Response {
                    method: "isPrime",
                    prime: is_prime(res.number)
                };
                
                let mut res_json = serde_json::to_vec(&response)?;
                res_json.push(b'\n');
                socket.write_all(&res_json).await?;

            } 
            else {
                socket.write_all(b"bye").await?;
                return Ok(());
            }

            buff.drain(..=pos);
        }
    }
}

pub async fn solve() -> io::Result<()> {
    let acc = TcpListener::bind("[::]:6969").await?;

    loop {
        let (socket, addr) = acc.accept().await?;
        println!("{addr} connected");

        tokio::spawn(async move {
            if let Err(e) = handle_req(socket, addr).await {
                eprintln!("{}", e);
            }
        });
    }
}