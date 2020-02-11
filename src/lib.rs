use chrono::{DateTime, NaiveDateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::{
    task::JoinHandle,
    stream::{self, Stream, StreamExt},
    sync::mpsc,
};
use tokio_binance::*;

pub type Kline = (DateTime<Utc>, Decimal, Decimal);
type Indicator = Option<(DateTime<Utc>, Decimal)>;

pub async fn klines<S: Into<String>, U: Into<String>>(symbol: S, interval: Interval, api_key: U) -> tokio_binance::error::Result<(JoinHandle<()>, impl Stream<Item=Kline> + Unpin + Send)> {
    let symbol: String = symbol.into();

    let client = MarketDataClient::connect(api_key, BINANCE_US_URL)?;
    let mut response: Value = client
        .get_candlestick_bars(&symbol, interval)
        .json()
        .await?;

    response.as_array_mut().unwrap().pop();

    let klines: Vec<_> = response
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .map(|v| (v[0].take(), v[1].take(), v[4].take()))
        .collect();

    let historic_stream = stream::iter(klines);
    let (mut tx, rx) = mpsc::channel(100);
    
    let handle = tokio::spawn(async move {

        let channel = Channel::Kline(&symbol, interval);
        let mut present_stream = match WebSocketStream::connect(channel, BINANCE_US_WSS_URL).await {
            Ok(s) => s,
            Err(e) => {
                eprint!("{}", e);
                return;
            }
        };

        loop {
            match present_stream.json::<Value>().await {
                Ok(Some(mut value)) => {
                    //println!("{}", serde_json::to_string_pretty(&value).unwrap());
                    if channel == value["stream"] && value["data"]["k"]["x"] == true {
                        let t = value["data"]["k"]["t"].take();
                        let o = value["data"]["k"]["o"].take();
                        let c = value["data"]["k"]["c"].take();
                        
                        if let Err(_) = tx.send((t, o, c)).await {
                            eprint!("kline receiver dropped");
                            return;
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("{}", e);
                    return;
                }
            };
        }
    });

    let chained_stream = historic_stream.chain(rx).map(|(t, o, c)| {
        (
            DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(t.as_i64().unwrap() / 1000, 0),
                Utc,
            ),
            o.as_str().unwrap().parse::<Decimal>().unwrap(),
            c.as_str().unwrap().parse::<Decimal>().unwrap(),
        )
    });

    Ok((handle, chained_stream))
}








#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
