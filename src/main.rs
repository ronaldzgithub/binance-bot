use futures::{join, stream::StreamExt, try_join};
use log::{info, warn, Level};
use rust_decimal::{prelude::*, Decimal};
use serde_json::Value;
use tokio_binance::*;

const MIN_NOTIONAL: usize = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::init_with_level(Level::Info).unwrap();
    let _api_key = "D3XovajhBd4mS0YIEwlPNpkituPvL2LCC1JogGLAd4djXZasUnbRthwUEwimtrOF";
    let _secret_key = "B2w5fAlmRBus9sxBYq5VyMLe69DiamCaX1WrSjXakhPEODtD2gEp3R1IaaKIHyNI";

    let symbol = "BNBUSDT";
    let rsi_buy: Decimal = 30.into();
    let rsi_sell: Decimal = 70.into();
    let min_notional = MIN_NOTIONAL.into();

    let mut rsi = binance_bot::rsi(symbol, Interval::OneHour, _api_key).await?;
    let mut macd = binance_bot::macd(symbol, Interval::OneHour, _api_key).await?;

    let mut buy_state = binance_bot::OrderState {
        prev_macd: None,
        confirm_count: 0,
    };
    let mut sell_state = binance_bot::OrderState {
        prev_macd: None,
        confirm_count: 0,
    };

    let client = AccountClient::connect(_api_key, _secret_key, BINANCE_US_URL)?;

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
                    let market_client = client.into_market_data_client();
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
                        .filter(|v| v["asset"] == "USDT")
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
                        let dp = 10.0f64.powi(2);
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
                    let market_client = client.into_market_data_client();
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
                        .filter(|v| v["asset"] == "BNB")
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
