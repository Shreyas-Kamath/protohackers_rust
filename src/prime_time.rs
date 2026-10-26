use serde::{Deserialize, Serialize};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Number {
    Integer(i64),
    Double(f64),
}

#[derive(Serialize, Deserialize, Debug)]
struct Request {
    method: String,
    number: Number,
}

#[derive(Serialize, Deserialize)]
struct Response {
    method: String,
    prime: bool,
}

fn is_prime(num: Number) -> bool {
    match num {
        Number::Integer(n) => {
            if n <= 1 {
                return false;
            }
            if n <= 3 {
                return true;
            }

            if n % 2 == 0 || n % 3 == 0 {
                return false;
            }

            let mut i: i64 = 5;

            while i * i <= n {
                if n % i == 0 || n % (i + 2) == 0 {
                    return false;
                }
                i += 6;
            }

            return true;
        }
        Number::Double(_) => {
            return false;
        }
    }
}

async fn handle_client(socket: TcpStream) -> io::Result<()> {
    let mut stream = BufReader::new(socket);
    let mut buffer: Vec<u8> = vec![];

    loop {
        let bytes_read = stream.read_until(b'\n', &mut buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        match serde_json::from_slice::<Request>(&buffer[..bytes_read - 1]) {
            Ok(request) => {
                println!("{:?}", request);

                if request.method != "isPrime" {
                    return Ok(());
                };

                let response = Response {
                    method: "isPrime".to_string(),
                    prime: is_prime(request.number),
                };

                let serialized = serde_json::to_string(&response).unwrap() + "\n";
                stream.write_all(serialized.as_bytes()).await?;
            }

            Err(_) => {
                return Ok(());
            }
        }

        buffer.drain(0..bytes_read);
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
