use tokio::{io::{self, AsyncReadExt, AsyncWriteExt, BufReader}, net::{TcpListener, TcpStream}};

enum Cipher {
    Reversebits,
    Xor(u8),
    Xorpos,
    Add(u8),
    Addpos,
}   

fn extract_cipher(data: &[u8]) -> Vec<Cipher> {
    println!("Cipher string (raw), is: {:02X?}", data);

    let mut ciphers: Vec<Cipher> = vec![];
    let mut index: usize = 0;

    while index < data.len() {
        match data[index] {
            0x01 => ciphers.push(Cipher::Reversebits),

            0x02 => {
                index += 1;
                ciphers.push(Cipher::Xor(data[index]));
            }

            0x03 => ciphers.push(Cipher::Xorpos),

            0x04 => {
                index += 1;
                ciphers.push(Cipher::Add(data[index]));
            }

            0x05 => ciphers.push(Cipher::Addpos),

            _ => unreachable!()
        }

        index += 1;
    }

    ciphers
}

fn decode(client_streampos: &mut usize, byte: u8, sequence: &[Cipher]) -> char {
    let mut byte = byte;

    for cipher in sequence.iter().rev() {
        byte = match cipher {
            Cipher::Add(val) => byte.wrapping_sub(*val),
            Cipher::Xor(val) => byte ^ *val,
            Cipher::Reversebits => byte.reverse_bits(),
            Cipher::Xorpos => byte ^ (*client_streampos as u8),
            Cipher::Addpos => byte.wrapping_sub(*client_streampos as u8),
        }
    }

    *client_streampos += 1;
    byte as char
}

fn encode(server_streampos: &mut usize, data: &str, sequence: &[Cipher]) -> Vec<u8> {
    let mut encoded: Vec<u8> = Vec::with_capacity(data.len());

    for ch in data.chars() {
        let mut ch = ch;

        for cipher in sequence {
            ch = match cipher {
                Cipher::Add(val) => (ch as u8).wrapping_add(*val) as char,
                Cipher::Xor(val) => (ch as u8 ^ *val) as char,
                Cipher::Reversebits => (ch as u8).reverse_bits() as char,
                Cipher::Xorpos => (ch as u8 ^ (*server_streampos as u8)) as char,
                Cipher::Addpos => (ch as u8).wrapping_add(*server_streampos as u8) as char,
            }
        }

        encoded.push(ch as u8);
        *server_streampos += 1;
    }

    let mut delim = b'\n';

    for cipher in sequence {
        delim = match cipher {
            Cipher::Add(val) => delim.wrapping_add(*val),
            Cipher::Xor(val) => delim ^ *val,
            Cipher::Reversebits => (delim).reverse_bits(),
            Cipher::Xorpos => delim ^ (*server_streampos as u8),
            Cipher::Addpos => (delim as u8).wrapping_add(*server_streampos as u8),
        }
    }

    *server_streampos += 1;

    encoded.push(delim);
    encoded    
}

async fn handle_client(tcpstream: TcpStream) -> io::Result<()> {
    let mut stream = BufReader::new(tcpstream);
    let mut buffer = vec![];

    loop {
        let byte = stream.read_u8().await?;

        if byte == 0x00 {
            match buffer.last() {
                Some(last) => {
                    if *last != 0x02 && *last != 0x04 { break; }
                },
                None => break,
            }
        }

        buffer.push(byte);
    }

    let sequence = extract_cipher(&buffer);

    buffer.clear();

    let mut client_streampos: usize = 0;
    let mut server_streampos: usize = 0;
    let mut decoded = String::new();
    let mut compare = vec![];

    loop {       

        loop {
            let byte = stream.read_u8().await?;
            compare.push(byte);

            let ch = decode(&mut client_streampos, byte, &sequence);

            if ch == '\n' { 
                compare.pop();
                break; 
            }
            decoded.push(ch);
        }

        if compare == decoded.as_bytes() {
            break;
        }

        let mut max_amt: i32 = 0;
        let mut answer = "";

        for entry in decoded.split(',') {
            match entry.split_once('x') {
                Some((amount, _)) => {
                    match amount.parse::<i32>() {
                        Ok(amount) => {
                            if amount > max_amt {
                                max_amt = amount;
                                answer = entry;
                            }
                        }

                        Err(e) => {
                            println!("Error: {e}");
                            println!("Entry was: {entry}");
                            println!("Decoded was: {decoded}");
                        }
                    }
                }

                None => {
                    println!("{entry}");
                }
            }
        }

        println!("{answer}");
        let encoded = encode(&mut server_streampos, answer, &sequence);
        stream.write_all(encoded.as_slice()).await?;

        decoded.clear();
        compare.clear();
    }

    Ok(())
}

pub async fn solve() -> io::Result<()> {
    let acceptor = TcpListener::bind("[::]:6969").await?;

    loop {
        if let Ok((stream, ..)) = acceptor.accept().await {
            tokio::spawn(async move {
                let _ = handle_client(stream).await;
            });
        }
    }
}