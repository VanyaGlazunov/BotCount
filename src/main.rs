use std::{
    collections::{HashMap, VecDeque}, convert::Infallible, env, sync::Arc, usize
};

use serde_json::json;
use tokio::sync::Mutex;

use serde::Deserialize;
use warp::{
    Filter,
    reject::Rejection,
};

#[derive(Debug, Clone, Copy, Deserialize)]
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
        self.user_events_count
            .entry(event.user_id)
            .and_modify(|c| *c += 1)
            .or_insert(1);
        if let Some(count) = self.user_events_count.get(&event.user_id)
            && *count == self.event_count_limit
        {
            self.bot_count += 1;
        }
    }

    fn count_bots(&mut self, timestamp: usize) -> usize {
        let old = timestamp - self.timestamp_threshold;
        while let Some(e) = self.events_timestamps.front().cloned()
            && e.timestamp < old
        {
            self.events_timestamps.pop_front();
            self.user_events_count
                .entry(e.user_id)
                .and_modify(|c| *c -= 1);
            if let Some(count) = self.user_events_count.get(&e.user_id)
                && *count == self.event_count_limit - 1
            {
                self.bot_count -= 1;
            }
        }

        self.bot_count
    }
}

type DB = Arc<Mutex<BotCounter>>;

async fn add_event(event: Event, db: DB) -> Result<impl warp::Reply, Rejection> {
    db.lock().await.add_event(event);
    Ok(warp::http::StatusCode::CREATED)
}

async fn get_count(query: TimestampQuery, db: DB) -> Result<impl warp::Reply, Rejection> {
    let count = db.lock().await.count_bots(query.timestamp);
    Ok(warp::reply::json(&json!({
        "bot_count": count,
    })))
}

fn with_db(db: DB) -> impl Filter<Extract = (DB,), Error = Infallible> + Clone {
    warp::any().map(move || db.clone())
}

#[tokio::main]
async fn main() {
    let args: Vec<_> = env::args().skip(1).collect();
    let timestamp_threshold = args[1].parse::<usize>().unwrap();
    let event_count_limit = args[2].parse::<usize>().unwrap();
    let db = Arc::new(Mutex::new(BotCounter::new(timestamp_threshold, event_count_limit)));

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

    let routes = add_event_route.or(get_count_route);

    println!("Starting server on http://127.0.0.1:3030");
    warp::serve(routes).run(([127, 0, 0, 1], 3080)).await
}
