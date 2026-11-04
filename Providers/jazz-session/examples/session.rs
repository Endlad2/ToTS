use jazz_session::{create_call, join_call};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let session = match args.as_slice() {
        [mode, name] if mode == "create" => create_call(name).await?,
        [mode, name, link] if mode == "join" => join_call(link, name).await?,
        _ => return Err("Использование: session create ИМЯ | session join ИМЯ ССЫЛКА".into()),
    };
    println!("{}", serde_json::to_string_pretty(&session.json().await)?);
    eprintln!("Сессия активна; Ctrl+C завершает звонок.");
    tokio::signal::ctrl_c().await?;
    session.close().await?;
    Ok(())
}
