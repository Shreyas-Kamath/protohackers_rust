use std::{collections::{HashMap, HashSet}, time::Duration};

use slab::Slab;
use tokio::{io::{self, AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::mpsc, task::JoinHandle, time::{self, Instant}};

#[derive(Debug)]
enum ClientType {
    IAmCamera {
        road: u16,
        mile: u16,
        limit: u16,
    },
    IAmDispatcher {
        numroads: u8,
        roads: Vec<u16>,
    },
}

enum ServerEvent {
    NewClient(TcpStream),
    Type {
        client_id: usize,
        t: ClientType,
    },
    Plate {
        client_id: usize,
        plate: String,
        timestamp: u32,
    },
    WantHeartbeat {
        client_id: usize,
        interval: u32,
    },
    Unknown(usize),
    NeedHeartbeat(usize),
    Disconnect(usize),
}

enum ClientEvent {
    Error,
    Ticket {
        plate: String,
        road: u16,
        mile1: u16,
        timestamp1: u32,
        mile2: u16,
        timestamp2: u32,
        speed: u16 // (100x mph)
    },
    Heartbeat,
}

enum HeartbeatTask {
    Zero,
    Enabled(JoinHandle<()>),
}

struct Client {
    channel: mpsc::Sender<ClientEvent>,
    t: Option<ClientType>,
    timer: Option<HeartbeatTask>,
}

#[derive(Hash, Eq, PartialEq)]
struct PlateRoad {
    plate: String,
    road: u16,
}

#[derive(Clone)]
struct Observation {
    mile: u16,
    timestamp: u32,
}

fn find_dispatcher(road: u16, road_map: &HashMap<u16, Vec<usize>>) -> Option<&usize> {
    road_map
        .get(&road)?
        .first()
}

fn try_detect_violation(vec: &mut Vec<Observation>, limit: u16, observed: &mut HashSet<u32>) -> Option<(Observation, Observation, u16)> {
    let latest = vec.last().unwrap();

    for index in 0..vec.len() - 1 {
        let previous = &vec[index];

        let delta_miles = (latest.mile as i64 - previous.mile as i64).abs();
        let delta_time = (latest.timestamp as i64 - previous.timestamp as i64).abs();

        let speed = delta_miles * 360000 / delta_time; 

        // println!("Speed: {}, limit: {}", speed, limit as i64 * 100);

        if speed > (limit as i64 * 100) {
            let day1 = latest.timestamp / 86400;
            let day2 = previous.timestamp / 86400;

            if observed.contains(&day1) || observed.contains(&day2) {
                continue;
            }

            observed.insert(day1);
            observed.insert(day2);

            let (first, second) = 
            if latest.timestamp > previous.timestamp {
                (previous, latest)
            }
            else {
                (latest, previous)
            };

            return Some((first.clone(), second.clone(), speed as u16));
        }
    }

    None
}

async fn listen(server_tx: mpsc::Sender<ServerEvent>) -> io::Result<()> {
    let listener = TcpListener::bind("[::]:6969").await?;

    loop {
        if let Ok((stream, ..)) = listener.accept().await {
            let _ = server_tx.send(ServerEvent::NewClient(stream)).await;
        }
    }
}

async fn read_loop(mut reader: OwnedReadHalf, server_tx: mpsc::Sender<ServerEvent>, client_id: usize) -> io::Result<()> {
    loop {
        let id = reader.read_u8().await?;

        match id {
            0x20 => {
                let len = reader.read_u8().await?;
                let mut raw_plate: Vec<u8> = vec![0u8; len as usize];
                reader.read_exact(&mut raw_plate).await?;
                let timestamp = reader.read_u32().await?;

                let plate = String::from_utf8(raw_plate).unwrap();

                let _ = server_tx.send(ServerEvent::Plate { client_id, plate, timestamp }).await;
            },

            0x40 => {
                let interval = reader.read_u32().await? * 100;
                let _ = server_tx.send(ServerEvent::WantHeartbeat { client_id, interval }).await;
            }

            0x80 => {
                let mut buffer = [0u8; 6];
                reader.read_exact(&mut buffer).await?;

                let road = u16::from_be_bytes(buffer[..2].try_into().unwrap());
                let mile: u16 = u16::from_be_bytes(buffer[2..4].try_into().unwrap());
                let limit = u16::from_be_bytes(buffer[4..6].try_into().unwrap());

                let _ = server_tx.send(ServerEvent::Type { client_id, t: ClientType::IAmCamera { road, mile, limit } }).await;
            },

            0x81 => {
                let numroads = reader.read_u8().await?;
                let mut buf: Vec<u8> = vec![0u8; numroads as usize * 2];
                let mut roads: Vec<u16> = Vec::with_capacity(buf.len() / 2);

                reader.read_exact(&mut buf).await?;

                for chunk in buf.chunks_exact(2) {
                    roads.push(u16::from_be_bytes(chunk.try_into().unwrap()));
                }

                let _ = server_tx.send(ServerEvent::Type { client_id, t: ClientType::IAmDispatcher { numroads, roads } }).await;
            }

            // should need an error? send an `Unknown` message?
            _ => {
                let _ = server_tx.send(ServerEvent::Unknown(client_id)).await;
            }
        }
    }
}

async fn write_loop(mut writer: OwnedWriteHalf, mut client_rx: mpsc::Receiver<ClientEvent>) -> io::Result<()> {
    while let Some(event) = client_rx.recv().await {
        match event {
            ClientEvent::Ticket { plate, road, mile1, timestamp1, mile2, timestamp2, speed } => {
                let mut buffer: Vec<u8> = Vec::with_capacity(1 + 1 + plate.len() + 2 + 2 + 4 + 2 + 4 + 2);
                buffer.push(0x21);
                buffer.push(plate.len() as u8);
                buffer.extend_from_slice(plate.as_bytes());
                buffer.extend_from_slice(&road.to_be_bytes());
                buffer.extend_from_slice(&mile1.to_be_bytes());
                buffer.extend_from_slice(&timestamp1.to_be_bytes());
                buffer.extend_from_slice(&mile2.to_be_bytes());
                buffer.extend_from_slice(&timestamp2.to_be_bytes());
                buffer.extend_from_slice(&speed.to_be_bytes());

                writer.write_all(&buffer).await?;
            }
            ClientEvent::Error => {
                writer.write_u8(0x10).await?;
                let mut buffer: Vec<u8> = vec![];
                buffer.push(3);
                buffer.extend_from_slice("bad".as_bytes());
                writer.write_all(&buffer).await?;
                break;
            }
            ClientEvent::Heartbeat => {
                writer.write_u8(0x41).await?;
            }
        }
    }

    Ok(())
}

async fn run_client(stream: TcpStream, client_rx: mpsc::Receiver<ClientEvent>, server_tx: mpsc::Sender<ServerEvent>, client_id: usize) -> io::Result<()> {
    let server_tx1 = server_tx.clone();

    let (reader, writer) = stream.into_split();

    let mut readloop = tokio::spawn(async move {
        let _ = read_loop(reader, server_tx1, client_id).await;
    });

    let mut writeloop = tokio::spawn(async move {
        let _ = write_loop(writer, client_rx).await;
    });

    tokio::select! {
        _ = &mut readloop => {
            writeloop.abort();
        }

        _ = &mut writeloop => {
            readloop.abort();
        }
    }

    let _ = server_tx.send(ServerEvent::Disconnect(client_id)).await;
    Ok(())
}

async fn server() -> io::Result<()> {
    let (server_tx, mut server_rx) = mpsc::channel::<ServerEvent>(512);
    let server_tx1 = server_tx.clone();

    let mut clients: Slab<Client> = Slab::new();
    let mut records: HashMap<PlateRoad, Vec<Observation>> = HashMap::new();
    let mut road_map: HashMap<u16, Vec<usize>> = HashMap::new();
    let mut waiting_tickets: HashMap<u16, Vec<ClientEvent>> = HashMap::new();
    let mut observed: HashMap<String, HashSet<u32>> = HashMap::new();

    tokio::spawn(listen(server_tx1));

    while let Some(event) = server_rx.recv().await {
        match event {
            ServerEvent::NewClient(stream) => {
                let (client_tx, client_rx) = mpsc::channel::<ClientEvent>(32);
                let client_id = clients.insert(Client { channel: client_tx, t: None, timer: None });

                let server_tx1 = server_tx.clone();
                tokio::spawn(async move {
                    let _ = run_client(stream, client_rx, server_tx1, client_id).await;
                });
            }
            ServerEvent::Type { client_id, t } => {
                let client = clients.get_mut(client_id).unwrap();
                if client.t.is_some() {
                    println!("{client_id} has sent multiple identities - kick");
                    let _ = client.channel.send(ClientEvent::Error).await;
                }
                else {
                    // println!("Assigned {:?} to {}", t, client_id);
                    client.t = Some(t);

                    if let Some(ClientType::IAmDispatcher { numroads, roads }) = &client.t {
                        for road in roads {
                            road_map.entry(*road).or_default().push(client_id);
                            
                            if let Some(tickets) = waiting_tickets.remove(road) {
                                for ticket in tickets {
                                    let _ = client.channel.send(ticket).await;
                                }
                            }
                        }
                    }
                }
            }
            ServerEvent::Plate { client_id, plate, timestamp } => {
                let client = clients.get(client_id).unwrap();

                if let Some(ClientType::IAmCamera { road, mile, limit }) = &client.t {
                    // println!("{} saw {} at {}", client_id, plate, timestamp);
                    let vec = records.entry(PlateRoad { plate: plate.clone(), road: *road }).or_default();
                    vec.push(Observation { mile: *mile, timestamp });

                    if let Some((previous, next, speed)) = try_detect_violation(vec, *limit, observed.entry(plate.clone()).or_default()) {
                        if let Some(dispatcher_id) = find_dispatcher(*road, &road_map) {
                            let dispatcher = clients.get(*dispatcher_id).unwrap();
                            let _ = dispatcher.channel.send(ClientEvent::Ticket { plate, road: *road, mile1: previous.mile, timestamp1: previous.timestamp, mile2: next.mile, timestamp2: next.timestamp, speed }).await;
                        }
                        // else store ticket
                        else {
                            waiting_tickets.entry(*road).or_default().push(ClientEvent::Ticket { plate, road: *road, mile1: previous.mile, timestamp1: previous.timestamp, mile2: next.mile, timestamp2: next.timestamp, speed });
                        }
                    }
                } 
                else {
                    let _ = client.channel.send(ClientEvent::Error).await;
                }
            }
            ServerEvent::WantHeartbeat { client_id, interval } => {
                let client = clients.get_mut(client_id).unwrap();

                if client.timer.is_some() {
                    println!("{client_id} has requested double heartbeat - kick");
                    let _ = client.channel.send(ClientEvent::Error).await;
                }
                else {
                    // spawn timer
                    let server_tx1 = server_tx.clone();
                    // println!("{} wants heartbeat: {} ms", client_id, interval);

                    if interval == 0 {
                        client.timer = Some(HeartbeatTask::Zero);
                    }
                    else {
                        client.timer = Some(HeartbeatTask::Enabled(tokio::spawn(async move {
                            let duration = Duration::from_millis(interval as u64);
                            let mut timer = time::interval_at(Instant::now() + duration, duration);

                            loop {
                                timer.tick().await;
                                let _ = server_tx1.send(ServerEvent::NeedHeartbeat(client_id)).await;
                            }
                        })));
                    }
                }
            }
            ServerEvent::Unknown(client_id) => {
                let client = clients.get(client_id).unwrap();
                let _ = client.channel.send(ClientEvent::Error).await;
            }
            ServerEvent::NeedHeartbeat(client_id) => {
                if let Some(client) = clients.get(client_id) {
                    let _ = client.channel.send(ClientEvent::Heartbeat).await;
                }
            }
            ServerEvent::Disconnect(client_id) => {
                // remove camera or dispatcher
                let mut client = clients.remove(client_id);

                if let Some(ClientType::IAmDispatcher { numroads: _, roads }) = client.t {
                    for road in roads {
                        let entry = road_map.get_mut(&road).unwrap();
                        entry.
                        iter()
                        .position(|id| *id == client_id)
                        .map(|found| entry.swap_remove(found));
                    }
                }

                if let Some(HeartbeatTask::Enabled(handle)) = client.timer.take() {
                    handle.abort();
                }
            }
        }
    }

    Ok(())
}


pub async fn solve() -> io::Result<()> {
    server().await
}