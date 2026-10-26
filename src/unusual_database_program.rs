use std::collections::{
    HashMap,
    hash_map::Entry::{Occupied, Vacant},
};

use tokio::{io, net::UdpSocket};

pub async fn solve() -> io::Result<()> {
    let socket = UdpSocket::bind("[::]:6969").await?;
    let mut db: HashMap<String, String> = HashMap::new();
    db.insert(
        "version".to_string(),
        "Ken's Key-Value Store 1.0".to_string(),
    );

    let mut buffer: Vec<u8> = vec![0u8; 1000];

    loop {
        match socket.recv_from(&mut buffer).await {
            Ok((bytes_recd, from)) => {
                match buffer[..bytes_recd].iter().position(|&b| b == b'=') {
                    Some(pos) => {
                        let key = String::from_utf8(buffer[0..pos].to_vec()).unwrap();
                        let val = String::from_utf8(buffer[pos + 1..bytes_recd].to_vec()).unwrap();

                        if key != "version" {
                            db.insert(key, val);
                        }
                    }

                    None => {
                        let mut out = String::from_utf8(buffer[..bytes_recd].to_vec()).unwrap();

                        match db.entry(out.clone()) {
                            Vacant(_) => {
                                out.push('=');
                                socket.send_to(out.as_bytes(), from).await?;
                            }

                            Occupied(e) => {
                                out.push('=');
                                out.push_str(e.get());
                                socket.send_to(out.as_bytes(), from).await?;
                            }
                        }
                    }
                }

                buffer.fill(0u8);
            }

            Err(_) => {}
        }
    }
}
