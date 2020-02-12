use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use tokio::{
    stream::{self, Stream},
    sync::mpsc,
};

use futures::{future, stream::StreamExt, join};

use tokio_binance::*;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;

pub type Kline = (DateTime<Utc>, Decimal, Decimal);
type Indicator = Option<(DateTime<Utc>, Decimal)>;

struct State {
    prev_ema: Option<Decimal>,
    window: Vec<Decimal>,
    count: usize,
}

pub async fn klines<S: Into<String>, U: Into<String>>(
    symbol: S,
    interval: Interval,
    api_key: U,
) -> tokio_binance::error::Result<impl Stream<Item = Kline> + Unpin + Send> {
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

    tokio::spawn(async move {
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

    Ok(chained_stream)
}

pub async fn ema<S: Stream<Item = Kline> + Unpin + Send>(
    klines: S,
    window_size: usize,
) -> tokio_binance::error::Result<impl Stream<Item = Indicator> + Unpin + Send> {
    //let klines = klines(symbol, interval, api_key).await?;
    let const1: Decimal = 1.into();
    let const2: Decimal = 2.into();
    let dwindow_size: Decimal = window_size.into();

    let k: Decimal = const2 / (dwindow_size + const1);

    let state = State {
        prev_ema: None,
        window: Vec::with_capacity(window_size),
        count: 0,
    };

    let ema = klines.scan(state, move |state, (t, _, c)| {
        if state.count == window_size - 1 {
            if let Some(ema) = state.prev_ema {
                let ema = c * k + ema * (const1 - k);
                state.prev_ema = Some(ema);
                future::ready(Some(Some((t, ema))))
            } else {
                state.window.push(c);
                let sma = state.window.iter().map(|x| *x).sum::<Decimal>() / dwindow_size;
                state.prev_ema = Some(sma);

                future::ready(Some(None))
            }
        } else {
            state.window.push(c);
            state.count += 1;

            future::ready(Some(None))
        }
    });

    Ok(ema)
}

pub async fn rsi<S: Into<String>, U: Into<String>>(
    symbol: S,
    interval: Interval,
    api_key: U,
) -> tokio_binance::error::Result<impl Stream<Item = Indicator> + Unpin + Send> {
    let symbol: String = symbol.into();
    let api_key: String = api_key.into();
    
    let size: usize = 14;
    let const1 = Decimal::zero();

    let (mut tx, rx) = mpsc::channel(100);

    let candles = klines(symbol.clone(), interval, api_key.clone()).await?;
    let gains = candles.map(move |mut x|if (x.2 - x.1).is_sign_positive() {x} else {x.2 = const1; x});
    let mut gains = ema(gains, size).await?;

    let candles = klines(symbol, interval, api_key).await?;
    let losses = candles.map(move |mut x|if (x.2 - x.1).is_sign_negative() {x} else {x.2 = const1; x});
    let mut losses = ema(losses, size).await?;

    tokio::spawn(async move {
        let const1: Decimal = 100.into();
        let const2: Decimal = 1.into();

        while let (Some(gains), Some(losses)) = join!(gains.next(), losses.next()) {

            if let (Some((_, gains)), Some((time, losses))) = (gains, losses) {
                let rsi = const1 - (const1 / (const2 + (gains / losses)));
                if let Err(_) = tx.send(Some((time, rsi))).await {
                    eprintln!("rsi receiver dropped");
                    return;
                }
            } else {
                if let Err(_) = tx.send(None).await {
                    eprintln!("rsi receiver dropped");
                    return;
                }
            }

        }
    });

    Ok(rx)
}

pub async fn macd<S: Into<String>, U: Into<String>>(
    symbol: S,
    interval: Interval,
    api_key: U,
) -> tokio_binance::error::Result<impl Stream<Item = Indicator> + Unpin + Send> {
    let symbol: String = symbol.into();
    let api_key: String = api_key.into();

    let (mut tx, rx) = mpsc::channel(100);

    let candles = klines(symbol.clone(), interval, api_key.clone()).await?;
    let mut ema12 = ema(candles, 12).await?;

    let candles = klines(symbol.clone(), interval, api_key.clone()).await?;
    let mut ema26 = ema(candles, 26).await?;

    tokio::spawn(async move {
        while let (Some(ema12), Some(ema26)) = join!(ema12.next(), ema26.next()) {
            
            if let (Some((_, ema12)), Some((time, ema26))) = (ema12, ema26) {
                let macd = ema12 - ema26;
                if let Err(_) = tx.send(Some((time, macd))).await {
                    eprintln!("macd receiver dropped");
                    return;
                }
            } else {
                if let Err(_) = tx.send(None).await {
                    eprintln!("macd receiver dropped");
                    return;
                } 
            }
        }
    });

    Ok(rx)
}
