use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    sync::Arc,
};

use clap::Parser;
use serde_json::json;
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};
use warp::Filter;

/// Bot detection service that monitors user events and identifies potential bots
/// based on event frequency within a time window.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Time window in seconds for bot detection
    /// Events older than this will be excluded from analysis
    #[arg(short, long)]
    timestamp_threshold: usize,

    /// Number of events required to classify a user as a bot
    /// If a user generates this many events within the time window, they're considered a bot
    #[arg(short, long)]
    event_count_limit: usize,

    /// Port to run the server on
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
struct Event {
    user_id: usize,
    timestamp: usize,
}

#[derive(Debug, Deserialize)]
struct TimestampQuery {
    timestamp: usize,
}

#[derive(Default, Clone)]
struct BotCounter {
    timestamp_threshold: usize,
    event_count_limit: usize,
    events_timestamps: VecDeque<Event>,
    user_events_count: HashMap<usize, usize>,
    bot_count: usize,
}

impl BotCounter {
    fn new(timestamp_threshold: usize, event_count_limit: usize) -> Self {
        Self {
            timestamp_threshold,
            event_count_limit,
            ..Default::default()
        }
    }

    fn add_event(&mut self, event: Event) {
        self.events_timestamps.push_back(event);
        let count = self
            .user_events_count
            .entry(event.user_id)
            .and_modify(|c| *c += 1)
            .or_insert(1);
        if *count == self.event_count_limit {
            self.bot_count += 1;
        }
    }

    fn count_bots(&mut self, timestamp: usize) -> usize {
        if timestamp < self.timestamp_threshold {
            return self.bot_count;
        }
        let old = timestamp - self.timestamp_threshold;
        while !self.events_timestamps.is_empty() {
            let user_id = if let Some(e) = self.events_timestamps.front()
                && e.timestamp <= old
            {
                e.user_id
            } else {
                break;
            };

            self.events_timestamps.pop_front();

            self.user_events_count
                .entry(user_id)
                .and_modify(|c| *c -= 1);

            if let Some(count) = self.user_events_count.get(&user_id)
                && *count == self.event_count_limit - 1
            {
                self.bot_count -= 1;
            }
        }

        self.bot_count
    }
}

type DB = Arc<Mutex<BotCounter>>;

async fn add_event(event: Event, db: DB) -> Result<impl warp::Reply, Infallible> {
    db.lock().await.add_event(event);
    Ok(warp::http::StatusCode::OK)
}

async fn get_count(query: TimestampQuery, db: DB) -> Result<impl warp::Reply, Infallible> {
    let count = db.lock().await.count_bots(query.timestamp);
    Ok(warp::reply::json(&json!({
        "bot_count": count,
    })))
}

fn with_db(db: DB) -> impl Filter<Extract = (DB,), Error = Infallible> + Clone {
    warp::any().map(move || db.clone())
}

fn create_routes(
    db: DB,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let add_event_route = warp::path("event")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_db(db.clone()))
        .and_then(add_event);

    let get_count_route = warp::path("bots")
        .and(warp::get())
        .and(warp::query::<TimestampQuery>())
        .and(with_db(db))
        .and_then(get_count);

    add_event_route.or(get_count_route)
}

async fn start_server(timestamp_threshold: usize, event_count_limit: usize, port: u16) {
    let db = Arc::new(Mutex::new(BotCounter::new(
        timestamp_threshold,
        event_count_limit,
    )));

    let routes = create_routes(db);

    warp::serve(routes).run(([127, 0, 0, 1], port)).await
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("Starting server on http://127.0.0.1:{}", args.port);
    start_server(args.timestamp_threshold, args.event_count_limit, args.port).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_single_event() {
        let mut counter = BotCounter::new(100, 3);
        let event = Event {
            user_id: 1,
            timestamp: 50,
        };

        counter.add_event(event);

        assert_eq!(counter.events_timestamps.len(), 1);
        assert_eq!(counter.user_events_count.get(&1), Some(&1));
        assert_eq!(counter.bot_count, 0);
    }

    #[test]
    fn test_user_becomes_bot() {
        let mut counter = BotCounter::new(100, 3);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 50,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 51,
        });
        assert_eq!(counter.bot_count, 0);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 52,
        });
        assert_eq!(counter.bot_count, 1);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 53,
        });
        assert_eq!(counter.bot_count, 1);
    }

    #[test]
    fn test_multiple_users_become_bots() {
        let mut counter = BotCounter::new(100, 2);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 50,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 51,
        });
        assert_eq!(counter.bot_count, 1);

        counter.add_event(Event {
            user_id: 2,
            timestamp: 52,
        });
        counter.add_event(Event {
            user_id: 2,
            timestamp: 53,
        });
        assert_eq!(counter.bot_count, 2);

        counter.add_event(Event {
            user_id: 3,
            timestamp: 54,
        });
        counter.add_event(Event {
            user_id: 3,
            timestamp: 55,
        });
        assert_eq!(counter.bot_count, 3);
    }

    #[test]
    fn test_count_bots_keeps_recent_events() {
        let mut counter = BotCounter::new(100, 2);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 50,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 60,
        });
        assert_eq!(counter.bot_count, 1);

        let count = counter.count_bots(120);
        assert_eq!(count, 1);
        assert_eq!(counter.events_timestamps.len(), 2);
    }

    #[test]
    fn test_count_bots_partial_removal() {
        let mut counter = BotCounter::new(100, 3);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 50,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 60,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 100,
        });
        assert_eq!(counter.bot_count, 1);

        let count = counter.count_bots(155);
        assert_eq!(count, 0);
        assert_eq!(counter.events_timestamps.len(), 2);
        assert_eq!(counter.user_events_count.get(&1), Some(&2));
    }

    #[test]
    fn test_complex_scenario() {
        let mut counter = BotCounter::new(50, 3);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 100,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 110,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 120,
        });
        assert_eq!(counter.bot_count, 1);

        counter.add_event(Event {
            user_id: 2,
            timestamp: 130,
        });
        counter.add_event(Event {
            user_id: 2,
            timestamp: 140,
        });
        assert_eq!(counter.bot_count, 1);

        counter.add_event(Event {
            user_id: 3,
            timestamp: 150,
        });
        counter.add_event(Event {
            user_id: 3,
            timestamp: 160,
        });
        counter.add_event(Event {
            user_id: 3,
            timestamp: 170,
        });
        assert_eq!(counter.bot_count, 2);

        let count = counter.count_bots(180);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_bot_counter_edge_case_exact_threshold() {
        let mut counter = BotCounter::new(100, 2);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 100,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 150,
        });
        assert_eq!(counter.bot_count, 1);

        let count = counter.count_bots(200);
        assert_eq!(count, 0);
        assert_eq!(counter.events_timestamps.len(), 1);
    }

    #[test]
    fn test_multiple_users_interleaved() {
        let mut counter = BotCounter::new(100, 2);

        counter.add_event(Event {
            user_id: 1,
            timestamp: 50,
        });
        counter.add_event(Event {
            user_id: 2,
            timestamp: 51,
        });
        counter.add_event(Event {
            user_id: 1,
            timestamp: 52,
        });
        counter.add_event(Event {
            user_id: 2,
            timestamp: 53,
        });

        assert_eq!(counter.bot_count, 2);
        assert_eq!(counter.user_events_count.get(&1), Some(&2));
        assert_eq!(counter.user_events_count.get(&2), Some(&2));
    }

    #[tokio::test]
    async fn test_add_event_endpoint() {
        let db = Arc::new(Mutex::new(BotCounter::new(100, 3)));
        let routes = create_routes(db.clone());

        let event = Event {
            user_id: 1,
            timestamp: 50,
        };

        let resp = warp::test::request()
            .method("POST")
            .path("/event")
            .json(&event)
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), 200);
        assert_eq!(db.lock().await.events_timestamps.len(), 1);
        assert_eq!(db.lock().await.user_events_count.get(&1), Some(&1));
    }

    #[tokio::test]
    async fn test_get_bots_endpoint() {
        let db = Arc::new(Mutex::new(BotCounter::new(100, 2)));
        let routes = create_routes(db.clone());

        db.lock().await.add_event(Event {
            user_id: 1,
            timestamp: 50,
        });
        db.lock().await.add_event(Event {
            user_id: 1,
            timestamp: 60,
        });

        let resp = warp::test::request()
            .method("GET")
            .path("/bots?timestamp=100")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["bot_count"], 1);
    }

    #[tokio::test]
    async fn test_integration_add_and_count() {
        let db = Arc::new(Mutex::new(BotCounter::new(100, 3)));
        let routes = create_routes(db);

        for i in 0..3 {
            let event = Event {
                user_id: 1,
                timestamp: 50 + i * 10,
            };
            let resp = warp::test::request()
                .method("POST")
                .path("/event")
                .json(&event)
                .reply(&routes)
                .await;
            assert_eq!(resp.status(), 200);
        }

        let resp = warp::test::request()
            .method("GET")
            .path("/bots?timestamp=100")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["bot_count"], 1);
    }

    #[tokio::test]
    async fn test_multiple_users_integration() {
        let db = Arc::new(Mutex::new(BotCounter::new(100, 2)));
        let routes = create_routes(db);

        for i in 0..2 {
            let event = Event {
                user_id: 1,
                timestamp: 50 + i * 10,
            };
            warp::test::request()
                .method("POST")
                .path("/event")
                .json(&event)
                .reply(&routes)
                .await;
        }

        for i in 0..2 {
            let event = Event {
                user_id: 2,
                timestamp: 70 + i * 10,
            };
            warp::test::request()
                .method("POST")
                .path("/event")
                .json(&event)
                .reply(&routes)
                .await;
        }

        let resp = warp::test::request()
            .method("GET")
            .path("/bots?timestamp=100")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["bot_count"], 2);
    }

    #[tokio::test]
    async fn test_bot_count_with_old_events_removed() {
        let db = Arc::new(Mutex::new(BotCounter::new(50, 2)));
        let routes = create_routes(db);

        warp::test::request()
            .method("POST")
            .path("/event")
            .json(&Event {
                user_id: 1,
                timestamp: 10,
            })
            .reply(&routes)
            .await;

        warp::test::request()
            .method("POST")
            .path("/event")
            .json(&Event {
                user_id: 1,
                timestamp: 20,
            })
            .reply(&routes)
            .await;

        let resp = warp::test::request()
            .method("GET")
            .path("/bots?timestamp=100")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["bot_count"], 0);
    }
}
