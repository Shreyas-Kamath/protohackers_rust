use tokio::io;

mod budget_chat;
mod means_to_an_end;
mod prime_time;
mod smoke_test;
mod unusual_database_program;
mod speed_daemon;
mod insecure_sockets_layer;
mod line_reversal;
mod job_centre;

#[tokio::main]
async fn main() -> io::Result<()> {
    let problem = std::env::args().nth(1).unwrap().parse::<i32>().unwrap();
    match problem {
        0 => smoke_test::solve().await?,
        1 => prime_time::solve().await?,
        2 => means_to_an_end::solve().await?,
        3 => budget_chat::solve().await?,
        4 => unusual_database_program::solve().await?,
        6 => speed_daemon::solve().await?,
        7 => line_reversal::solve().await?,
        8 => insecure_sockets_layer::solve().await?,
        9 => job_centre::solve().await?,
        _ => unreachable!(),
    }

    Ok(())
}
