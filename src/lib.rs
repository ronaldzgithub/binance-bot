use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use tokio::{
    stream::{self, Stream},
    sync::mpsc,
};

use futures::{future, stream::StreamExt, join};

use tokio_binance::*;
use rust_decimal::Decimal;
use log::error;

pub type Kline = (DateTime<Utc>, Decimal, Decimal, &'static str);
type Indicator = Option<(DateTime<Utc>, Decimal, &'static str)>;

pub struct OrderState {
    pub prev_macd: Option<Decimal>,
    pub confirm_count: usize
}

struct State {
    prev_ema: Option<Decimal>,
    window: Vec<Decimal>,
    count: usize,
}

pub fn validator(x: String) -> Result<(), String> {
    if let Ok(x) = x.parse::<u32>() {
        if x <= 100 {
            Ok(())
        } else {
            Err(String::from("value has to be between 0-100"))
        }
    } else {
        Err(String::from("value has to be between 0-100"))
    }
}

pub fn process_info<'a>(symbol: &str, info: &'a Value) -> (i32, Decimal, &'a str, &'a str) {
    let symbol = info["symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["symbol"] == symbol)
        .last()
        .unwrap();

    let min_notional_filter = symbol["filters"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["filterType"] == "MIN_NOTIONAL")
        .last()
        .unwrap();

    let lot_size_filter = symbol["filters"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["filterType"] == "LOT_SIZE")
        .last()
        .unwrap();

    let min_notional = min_notional_filter["minNotional"]
        .as_str()
        .unwrap()
        .parse::<Decimal>()
        .unwrap();

    let lot_size = lot_size_filter["minQty"]
        .as_str()
        .unwrap()
        .parse::<f64>()
        .unwrap();

    let lot_size = (lot_size.log10() / 10.00f64.log10()).abs() as i32;
    let base_asset = symbol["baseAsset"].as_str().unwrap();
    let quote_asset = symbol["quoteAsset"].as_str().unwrap();

    (lot_size, min_notional, base_asset, quote_asset)
}

pub fn symbol_exist(symbol: &str, info: &Value) -> bool {
    let filter: Vec<_> = info["symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["symbol"] == symbol)
        .collect();

    !filter.is_empty()
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
        .map(|v| (v[0].take(), v[1].take(), v[4].take(), "old"))
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
                        if let Err(_) = tx.send((t, o, c, "new")).await {
                            error!("kline receiver dropped");
                            return;
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    error!("{}", e);
                    return;
                }
            };
        }
    });

    let chained_stream = historic_stream.chain(rx).map(|(t, o, c, s)| {
        (
            DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(t.as_i64().unwrap() / 1000, 0),
                Utc,
            ),
            o.as_str().unwrap().parse::<Decimal>().unwrap(),
            c.as_str().unwrap().parse::<Decimal>().unwrap(),
            s
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

    let ema = klines.scan(state, move |state, (t, _, c, s)| {
        if state.count == window_size - 1 {
            if let Some(ema) = state.prev_ema {
                let ema = c * k + ema * (const1 - k);
                state.prev_ema = Some(ema);
                future::ready(Some(Some((t, ema, s))))
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
    let zero = Decimal::new(1, 10);

    let (mut tx, rx) = mpsc::channel(100);

    let candles = klines(symbol.clone(), interval, api_key.clone()).await?;
    let gains = candles.map(move |mut x|if (x.2 - x.1).is_sign_positive() {x} else {x.2 = zero; x});
    let mut gains = ema(gains, size).await?;

    let candles = klines(symbol, interval, api_key).await?;
    let losses = candles.map(move |mut x|if (x.2 - x.1).is_sign_negative() {x} else {x.2 = zero; x});
    let mut losses = ema(losses, size).await?;

    tokio::spawn(async move {
        let const1: Decimal = 100.into();
        let const2: Decimal = 1.into();

        while let (Some(gains), Some(losses)) = join!(gains.next(), losses.next()) {
            if let (Some((_, gains, _)), Some((time, losses, s))) = (gains, losses) {
                let rsi = const1 - (const1 / (const2 + (gains / losses)));
                if let Err(_) = tx.send(Some((time, rsi, s))).await {
                    error!("rsi receiver dropped");
                    return;
                }
            } else {
                if let Err(_) = tx.send(None).await {
                    error!("rsi receiver dropped");
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
            
            if let (Some((_, ema12, _)), Some((time, ema26, s))) = (ema12, ema26) {
                let macd = ema12 - ema26;
                if let Err(_) = tx.send(Some((time, macd, s))).await {
                    error!("macd receiver dropped");
                    return;
                }
            } else {
                if let Err(_) = tx.send(None).await {
                    error!("macd receiver dropped");
                    return;
                } 
            }
        }
    });

    Ok(rx)
}
