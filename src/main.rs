use tokio::io;

mod smoke_test;
mod prime_time;
mod means_to_an_end;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let problem = &args[1].parse::<i32>().unwrap();

    match problem {
        0 => smoke_test::solve().await?,
        1 => prime_time::solve().await?,
        2 => means_to_an_end::solve().await?,
        _ => unreachable!(),
    }

    Ok(())
}
