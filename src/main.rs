use futures::{join, stream::StreamExt, try_join};
use log::{info, warn, error, Level};
use rust_decimal::{prelude::*, Decimal};
use clap::{Arg, App};
use serde_json::Value;
use tokio_binance::*;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("binance-bot")
        .version("0.1.0")
        .author("kgeronim <kevin.geronimo@outlook.com>")
        .about("Binance MACD-RSI Mean Reversion Trading Bot")
        .arg(Arg::with_name("interval")
            .short("i")
            .long("interval")
            .takes_value(true)
            .possible_values(&["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]))
        .arg(Arg::with_name("SYMBOL")
            .help("Trading pair that will be used for trading")
            .required(true)
            .index(1))
        .arg(Arg::with_name("RSI-BUY")
            .help("Buy at or above the provided rsi")
            .required(true)
            .index(2)
            .validator(binance_bot::validator))
        .arg(Arg::with_name("RSI-SELL")
            .help("Sell at or below the provided rsi")
            .required(true)
            .index(3)
            .validator(binance_bot::validator))
        .arg(Arg::with_name("debug")
            .short("d")
            .help("Print debug information verbosely"))
        .get_matches();

    if matches.is_present("debug") {
        simple_logger::init_with_level(Level::Debug).unwrap();
    } else {
        simple_logger::init_with_level(Level::Info).unwrap();
    }

    let symbol = matches.value_of("SYMBOL").unwrap();
    let rsi_buy = matches.value_of("RSI-BUY").map(|x| x.parse::<Decimal>().unwrap()).unwrap();
    let rsi_sell = matches.value_of("RSI-SELL").map(|x| x.parse::<Decimal>().unwrap()).unwrap();
    let interval = matches.value_of("interval").map_or(Interval::OneHour, |x| {
        serde_json::from_value(Value::String(x.into())).unwrap()
    });

    if rsi_buy >= rsi_sell {
        error!("Buy low sell high not the other way around...");
        panic!()
    }

    let api_key = if let Ok(val) = std::env::var("API_KEY") { val } else {
        error!("API_KEY environment variable required");
        panic!() 
    };

    let secret_key = if let Ok(val) = std::env::var("SECRET_KEY") { val } else {
        error!("SECRET_KEY environment variable required");
        panic!() 
    };

    let client = AccountClient::connect(&api_key, secret_key, BINANCE_US_URL)?;

    let info = client
        .to_general_client()
        .get_exchange_info()
        .json::<Value>()
        .await?;

    if binance_bot::symbol_exist(symbol, &info) {
        if !symbol.contains("USD") {
            error!("Quote Asset must be USD or a stable coin");
            panic!()
        }
    } else {
        error!("Invalid Symbol. Base and quote assets are concatinated. e.g. BNBUSDT");
        panic!()
    }

    let (lot_size, min_notional, base_asset, quote_asset) = binance_bot::process_info(symbol, &info);
    info!("lot_size: {}, min_notional: {}, base_asset: {}, quote_asset: {}", lot_size, min_notional, base_asset, quote_asset);

    let mut rsi = binance_bot::rsi(symbol, interval, &api_key).await?;
    let mut macd = binance_bot::macd(symbol, interval, &api_key).await?;

    let mut buy_state = binance_bot::OrderState {
        prev_macd: None,
        confirm_count: 0,
    };

    let mut sell_state = binance_bot::OrderState {
        prev_macd: None,
        confirm_count: 0,
    };

    while let (Some(rsi), Some(macd)) = join!(rsi.next(), macd.next()) {
        if let (Some((time, rsi, "new")), Some((_, macd, _))) = (rsi, macd) {
            info!("time: {}, rsi: {}, macd: {}", time, rsi, macd);

            // Buy signals happen here
            if rsi < rsi_buy {
                if let None = buy_state.prev_macd {
                    buy_state.prev_macd = Some(macd);
                    info!("RSI buy signal, time: {}, rsi: {}", time, rsi)
                }
            }

            if let Some(pm) = buy_state.prev_macd {
                if macd > pm {
                    buy_state.confirm_count += 1;
                    info!(
                        "MACD buy signal, time: {}, macd: {}, prev_macd: {}, confirm_count: {}",
                        time, macd, pm, buy_state.confirm_count
                    );
                }
                buy_state.prev_macd = Some(macd);
            }

            if buy_state.confirm_count > 1 {
                loop {
                    let market_client = client.to_market_data_client();
                    let book_ticker = market_client
                        .get_order_book_ticker()
                        .with_symbol(symbol)
                        .json::<Value>();
    
                    let account = client.get_account().json::<Value>();
    
                    let (book_ticker, account) = try_join!(book_ticker, account)?;
    
                    let ask = book_ticker["askPrice"]
                        .as_str()
                        .unwrap()
                        .parse::<Decimal>()
                        .unwrap();
    
                    let balance = account["balances"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|v| v["asset"] == quote_asset)
                        .last()
                        .unwrap();
    
                    let balance = balance["free"]
                        .as_str()
                        .unwrap()
                        .parse::<Decimal>()
                        .unwrap();
    
                    info!("Check, Notional: {}", balance);
                    if balance > min_notional {
                        let size = balance / ask;
                        let size = size.to_f64().unwrap();
                        let dp = 10.0f64.powi(lot_size);
                        let size = (dp * size).floor() / dp;
    
                        let ask = ask.to_f64().unwrap();
                        let response = client
                            .place_limit_order(symbol, Side::Buy, ask, size, true)
                            .with_time_in_force(TimeInForce::Ioc)
                            .json::<Value>()
                            .await;
    
                        info!(
                            "Buy order, ask: {}, rsi: {}, macd: {}, confirmations: {}",
                            ask, rsi, macd, buy_state.confirm_count
                        );
                        match response {
                            Ok(res) => info!("{}", serde_json::to_string_pretty(&res).unwrap()),
                            Err(e) => {
                                warn!("Buy order cancelled: {}", e); 
                                break
                            }
                        }
                    } else {
                        info!("No funds, Notional: {}", balance);
                        break
                    }
                }
                
                buy_state.confirm_count = 0;
                buy_state.prev_macd = None;
            }

            // Sell signals happen here
            if rsi > rsi_sell {
                if let None = sell_state.prev_macd {
                    sell_state.prev_macd = Some(macd);
                    info!("RSI sell signal, time: {}, rsi: {}", time, rsi)
                }
            }

            if let Some(pm) = sell_state.prev_macd {
                if macd < pm {
                    sell_state.confirm_count += 1;
                    info!(
                        "MACD sell signal, time: {}, macd: {}, prev_macd: {}, confirm_count: {}",
                        time, macd, pm, sell_state.confirm_count
                    );
                }
                sell_state.prev_macd = Some(macd);
            }

            if sell_state.confirm_count > 1 {
                loop {
                    let market_client = client.to_market_data_client();
                    let book_ticker = market_client
                        .get_order_book_ticker()
                        .with_symbol(symbol)
                        .json::<Value>();
    
                    let account = client.get_account().json::<Value>();
    
                    let (book_ticker, account) = try_join!(book_ticker, account)?;
    
                    let bid = book_ticker["bidPrice"]
                        .as_str()
                        .unwrap()
                        .parse::<Decimal>()
                        .unwrap();
    
                    let balance = account["balances"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|v| v["asset"] == base_asset)
                        .last()
                        .unwrap();
    
                    let balance = balance["free"]
                        .as_str()
                        .unwrap()
                        .parse::<Decimal>()
                        .unwrap();
    
                    let notional = balance * bid;
                    info!("Check, Notional: {}", notional);
    
                    if notional > min_notional {
                        let size = balance.to_f64().unwrap();
                        let bid = bid.to_f64().unwrap();
    
                        let response = client
                            .place_limit_order(symbol, Side::Sell, bid, size, true)
                            .with_time_in_force(TimeInForce::Ioc)
                            .json::<Value>()
                            .await;
    
                        info!(
                            "Sell order, bid: {}, rsi: {}, macd: {}, confirmations: {}",
                            bid, rsi, macd, sell_state.confirm_count
                        );
                        match response {
                            Ok(res) => info!("{}", serde_json::to_string_pretty(&res).unwrap()),
                            Err(e) => {
                                warn!("Sell order cancelled: {}", e);
                                break
                            }
                        }
                    } else {
                        info!("No funds, Notional: {}", notional);
                        break
                    }
                }

                sell_state.confirm_count = 0;
                sell_state.prev_macd = None;
            }
        }
    }

    Ok(())
}
