use std::collections::{BinaryHeap, HashMap};

use serde::{Deserialize, Serialize};
use slab::Slab;
use tokio::{io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::mpsc};

enum ServerEvent {
    NewClient(TcpStream),
    Disconnect(usize),
    Query {
        user_id: usize,
        data: String
    },
}

enum UserEvent {
    Response(String),
}

#[derive(Serialize, Debug)]
struct QueryError {
    status: String,
    error: String,
}

#[derive(Clone)]
struct Job {
    id: u32,
    priority: u32,
    value: serde_json::Value,
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for Job {}

#[derive(Deserialize, Debug)]
#[serde(tag = "request", rename_all = "lowercase")]
enum QueryType {
    Get(GetRequest),
    Put(PutRequest),
    Delete(DeleteRequest),
    Abort(AbortRequest),
}

#[derive(Deserialize, Debug)]
struct GetRequest {
    queues: Vec<String>,
    wait: Option<bool>,
}

#[derive(Deserialize, Debug)]
struct PutRequest {
    queue: String,
    job: serde_json::Value,
    pri: u32,
}

#[derive(Deserialize, Debug)]
struct DeleteRequest {
    id: u32,
}

#[derive(Deserialize, Debug)]
struct AbortRequest {
    id: u32,
}

#[derive(Serialize, Debug)]
struct PutResponse {
    status: String,
    id: u32,
}

#[derive(Serialize, Debug)]
struct GetResponse {
    status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    job: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pri: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<String>,
}

#[derive(Serialize, Debug)]
struct DeleteResponse {
    status: String,
}

#[derive(Serialize, Debug)]
struct AbortResponse {
    status: String,
}

struct User {
    active: Vec<(Job, String)>,
    channel: mpsc::Sender<UserEvent>,
}

async fn try_send_to_waiters(users: &mut Slab<User>, waiters: &mut Vec<(usize, Vec<String>)>, jobs: &mut HashMap<String, BinaryHeap<Job>>, jobs_status: &mut Vec<(bool, Option<usize>)>) {
    let mut i = 0;

    while i < waiters.len() {
        let (user_id, queues) = &waiters[i];
        let mut found = false;

        for queue in queues {
            if found {
                break;
            }
            if let Some(heap) = jobs.get_mut(queue) {
                while let Some(job) = heap.pop() {
                    if jobs_status[job.id as usize].0 {
                        continue;
                    }

                    found = true;

                    let user = users.get_mut(*user_id).unwrap();
                    jobs_status[job.id as usize].1 = Some(*user_id);
                    user.active.push((job.clone(), queue.clone()));
                    let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&GetResponse { status: "ok".to_string(), id: Some(job.id), job: Some(job.value), pri: Some(job.priority), queue: Some(queue.clone())}).unwrap() + "\n")).await;
                    break;
                }
            }
        }

        if found {
            waiters.swap_remove(i);
        }
        else {
            i += 1;
        }
    }
}

fn try_abort_request(user: &mut User, job_id: u32) -> Option<(Job, String)> {
    user.active
        .iter()
        .position(|j| j.0.id == job_id)
        .map(|found| Some(user.active.swap_remove(found)))?
}

fn insert_job(queue: String, jobs: &mut HashMap<String, BinaryHeap<Job>>, job: Job) {
    jobs.entry(queue).or_default().push(job);
}

fn get_job(queues: &[String], jobs: &mut HashMap<String, BinaryHeap<Job>>, jobs_status: &mut Vec<(bool, Option<usize>)>, user_id: usize) -> Option<(Job, String)> {
    let mut max_pri: Option<u32> = None;
    let mut target_queue: Option<&String> = None;

    for queue in queues {
        if let Some(heap) = jobs.get_mut(queue) {
            while let Some(top_job) = heap.peek() {
                if jobs_status[top_job.id as usize].0 {
                    heap.pop();
                    continue;
                }

                if max_pri.is_none_or(|curr_max| top_job.priority > curr_max) {
                    max_pri = Some(top_job.priority);
                    target_queue = Some(queue);
                }
                break;
            }
        }
    }

    target_queue.and_then(|q| {
        jobs
            .get_mut(q)
            .and_then(|heap| heap.pop())
            .map(|job| {
                jobs_status[job.id as usize].1 = Some(user_id);
                (job, q.clone())
            })
    }) 
}

async fn write_loop(mut user_rx: mpsc::Receiver<UserEvent>, mut writer: OwnedWriteHalf) -> io::Result<()> {
    while let Some(event) = user_rx.recv().await {
        match event {
            UserEvent::Response(resp) => {
                writer.write_all(resp.as_bytes()).await?;
            }
        }
    }

    Ok(())
}

async fn read_loop(user_id: usize, server_tx: mpsc::Sender<ServerEvent>, reader: OwnedReadHalf) -> io::Result<()> {
    let mut buffer = String::new();
    let mut stream = BufReader::new(reader);

    loop {
        let bytes_read = stream.read_line(&mut buffer).await?;
        if bytes_read == 0 {
            break Ok(());
        }
        let to_send = std::mem::take(&mut buffer);
        let _ = server_tx.send(ServerEvent::Query { user_id, data: to_send }).await;
    }
}

async fn client(id: usize, rx: mpsc::Receiver<UserEvent>, server_tx: mpsc::Sender<ServerEvent>, stream: TcpStream) -> io::Result<()> {
    let server_tx1 = server_tx.clone();

    let (reader, writer) = stream.into_split();

    let mut writeloop = tokio::spawn(async move {
        let _ = write_loop(rx, writer).await;
    });

    let mut readloop = tokio::spawn(async move {
        let _ = read_loop(id, server_tx1, reader).await;
    });

    tokio::select! {
        _ = &mut writeloop => {
            readloop.abort();
        }

        _ = &mut readloop => {
            writeloop.abort();
        }
    }

    let _ = server_tx.send(ServerEvent::Disconnect(id)).await;
    Ok(())
}

async fn listen(server_tx: mpsc::Sender<ServerEvent>) -> io::Result<()> {
    let acceptor = TcpListener::bind("[::]:6969").await?;

    loop {
        if let Ok((stream, ..)) = acceptor.accept().await {
            let _ = server_tx.send(ServerEvent::NewClient(stream)).await;
        }
    }
}

async fn server() -> io::Result<()> {
    let mut unique_job_id: u32 = 0;
    let (server_tx, mut server_rx) = mpsc::channel::<ServerEvent>(2048);
    let mut jobs: HashMap<String, BinaryHeap<Job>> = HashMap::new();
    let mut users: Slab<User> = Slab::new();
    let mut waiters: Vec<(usize, Vec<String>)> = vec![];
    let mut job_status: Vec<(bool, Option<usize>)> = vec![];

    let server_tx1 = server_tx.clone();

    tokio::spawn(async move {
        let _ = listen(server_tx1).await;
    });

    loop {
        while let Some(event) = server_rx.recv().await {
            match event {
                ServerEvent::NewClient(stream) => {
                    let (user_tx, user_rx) = mpsc::channel::<UserEvent>(512);
                    let server_tx1 = server_tx.clone();
                    let id = users.insert(User { active: vec![], channel: user_tx });

                    tokio::spawn(async move {
                        let _ = client(id, user_rx, server_tx1.clone(), stream).await;
                    });
                }

                ServerEvent::Disconnect(user_id) => {
                    let user = users.remove(user_id);

                    for (job, heap) in user.active {
                        jobs.get_mut(&heap).unwrap().push(job);
                    }
                    
                    waiters
                        .iter()
                        .position(|w| w.0 == user_id)
                        .map(|w| waiters.swap_remove(w));

                    try_send_to_waiters(&mut users, &mut waiters, &mut jobs, &mut job_status).await;
                }

                ServerEvent::Query { user_id, data } => {
                    let user = users.get_mut(user_id).unwrap();

                    match serde_json::from_str::<QueryType>(&data[..data.len() - 1]) {
                        Ok(json) => {
                            match json {
                                QueryType::Get(GetRequest { queues, wait }) => {
                                    let job = get_job(&queues, &mut jobs, &mut job_status, user_id);

                                    match job {
                                        Some((j, queue)) => {
                                            user.active.push((j.clone(), queue.clone()));
                                            job_status[j.id as usize] = (false, Some(user_id));
                                            let _ =  user.channel.send(UserEvent::Response(serde_json::to_string::<GetResponse>(&GetResponse { status: "ok".to_string(), id: Some(j.id), job: Some(j.value), pri: Some(j.priority), queue: Some(queue) }).unwrap() + "\n")).await;
                                        }

                                        None => {
                                            if let Some(wait) = wait {
                                                if wait {
                                                    waiters.push((user_id, queues));
                                                }
                                                else {
                                                    let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&GetResponse { status: "no-job".to_string(), id: None, job: None, pri: None, queue: None }).unwrap() + "\n")).await;
                                                }
                                            }
                                            else {
                                                let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&GetResponse { status: "no-job".to_string(), id: None, job: None, pri: None, queue: None }).unwrap() + "\n")).await;
                                            }
                                        }
                                    }
                                }

                                QueryType::Put(PutRequest { queue, job, pri }) => {
                                    let job = Job {
                                        id: unique_job_id,
                                        priority: pri,
                                        value: job
                                    };

                                    insert_job(queue, &mut jobs, job);
                                    job_status.push((false, None));

                                    let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&PutResponse { status: "ok".to_string(), id: unique_job_id }).unwrap() + "\n")).await;

                                    unique_job_id += 1;

                                    try_send_to_waiters(&mut users, &mut waiters, &mut jobs, &mut job_status).await;
                                }

                                QueryType::Delete(DeleteRequest { id }) => {
                                    match job_status.get_mut(id as usize) {
                                        Some((deleted, owner)) => {
                                            if *deleted {
                                                let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&DeleteResponse { status: "no-job".to_string() }).unwrap() + "\n")).await;
                                            }
                                            else {
                                                *deleted = true;
                                                let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&DeleteResponse { status: "ok".to_string() }).unwrap() + "\n")).await;

                                                owner
                                                    .and_then(|worker| users.get_mut(worker))
                                                    .and_then(|user| {
                                                        let index = user.active.iter().position(|(job, ..)| job.id == id)?;
                                                        Some((user, index))
                                                    })
                                                    .map(|(user, index)| user.active.swap_remove(index));
                                            }
                                        }
                                        None => {
                                            let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&DeleteResponse { status: "no-job".to_string() }).unwrap() + "\n")).await;
                                        }
                                    }
                                }

                                QueryType::Abort(AbortRequest { id }) => {
                                    if let Some((job, owner)) = try_abort_request(user, id) {
                                        let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&AbortResponse { status: "ok".to_string() }).unwrap() + "\n")).await;
                                        jobs.get_mut(&owner).unwrap().push(job);
                                        try_send_to_waiters(&mut users, &mut waiters, &mut jobs, &mut job_status).await;
                                    }
                                    else {
                                        let _ = user.channel.send(UserEvent::Response(serde_json::to_string(&AbortResponse { status: "no-job".to_string() }).unwrap() + "\n")).await;
                                    }
                                }
                            }
                        }

                        Err(_) => {
                            let serialized = serde_json::to_string(&QueryError { status: "error".to_string(), error: "Unrecognised request type.".to_string() }).unwrap() + "\n";
                            let _ = user.channel.send(UserEvent::Response(serialized)).await;
                        }
                    }
                }
            }
        }
    }
}

pub async fn solve() -> io::Result<()> {
    server().await
}