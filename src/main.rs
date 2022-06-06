use std::sync::Arc;
use std::boxed::Box;
use log::*;
use actix_web::{middleware::Logger, HttpServer, web::Data};

pub mod redis_client {
    use redis::AsyncCommands;
    use redis::streams::StreamMaxlen;
    use redis_async_pool::RedisPool;
    use redis::RedisResult;

    #[derive(Clone)]
    pub struct RedisClient {
        pool: RedisPool
    }

    impl RedisClient {
        pub fn new(pool: RedisPool) -> RedisClient {
            RedisClient { pool }
        }

        pub async fn insert_into_stream<A: AsRef<str>, B: AsRef<str>>(
            &self, 
            stream_name: A,
            content: B,
            message_key: Option<String>,
            length_limit: Option<StreamMaxlen>
        ) -> Result<String, Box<dyn std::error::Error>> {
            let mut conn = self.pool.get().await?;
            let message_key: String = match message_key {
                Some(v) => v,
                None => String::from("*")
            };

            let add_result: RedisResult<String> = match length_limit {
                None => {
                    conn.xadd(stream_name.as_ref(), message_key, &[("message", content.as_ref())]).await
                },
                Some(opts) => {
                    conn.xadd_maxlen(
                        stream_name.as_ref(),
                        opts,
                        message_key,
                        &[("message", content.as_ref())]
                    ).await
                }
            };
            Ok(add_result?)
        }
    }
}

pub mod config {
    use serde::{Serialize, Deserialize};
    use serde::de::DeserializeOwned;
    use std::{
        fs::File,
        io::Read,
        collections::HashMap,
        env
    };
    use log::*;
    use serde_value::Value;

    #[derive(Debug, Clone)]
    pub struct Config {
        file: ConfigFile
    }
    
    type ConfigCreateResult = Result<Config, Box<dyn std::error::Error>>;

    impl Config {
        pub fn new<T: AsRef<str>>(path: T) -> ConfigCreateResult {
            let mut file = File::open(path.as_ref()).unwrap();
            let string = {
                let mut a = String::new();
                file.read_to_string(&mut a).unwrap();
                a
            };
            let file: ConfigFile = toml::from_str(string.as_str()).unwrap();
            Ok(Config { file })
        }

        pub fn get_variable<T: std::str::FromStr + DeserializeOwned, R: AsRef<str>>(&self, key: R) -> Option<T> {
            let mut value: Option<Value> = None;
            debug!("Finding value {} in env...", key.as_ref());
            if self.file.env_mapping.contains_key(key.as_ref()) {
                value = match env::var(
                    self.file.env_mapping.get(key.as_ref()).unwrap()
                ) {
                    Ok(value) => {
                        debug!("Found value {} in env!", key.as_ref());
                        Some(Value::String(value))   
                    },
                    Err(_) => None
                };
            }

            if value.is_none() {
                debug!("Finding value {} in config...", key.as_ref());
                let map = match serde_value::to_value(self.file.clone()) {
                    Ok(Value::Map(map)) => map,
                    _ => unimplemented!()
                };

                let map_key = Value::String(String::from(key.as_ref()));
                if map.contains_key(&map_key) {
                    debug!("Found value {} in config!", key.as_ref());
                    value = Some(map[&map_key].clone());
                }
            }

            debug!("Value {}: {:?}", key.as_ref(), value);
            let value: Value = value?;

            let deserialize_result = T::deserialize(value.clone());
            if let Ok(v) = deserialize_result {
                return Some(v)
            }

            warn!("Failed to deserialize {}", key.as_ref());
                    
            let string: String = match value {
                Value::String(v) => v,
                _ => return None
            };

            let str_value = string.as_str();
            if let Ok(v) = T::from_str(str_value) {
                debug!("Successfully parsed value from string!");
                return Some(v)
            } 
            None            
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct ConfigFile {
        server_ip: String,
        server_port: usize,

        log_level: String,

        workers_amount: usize,
        handlers_amount: usize,

        redis_connections: usize,
        redis_url: String,

        group_id: u32,
        confirmation: String,
        secret: String,

        stream_length: Option<usize>,
        stream_name: String,

        env_mapping: HashMap<String, String>
    }

}

pub mod redis_pool {
    use redis::Client;
    use redis::RedisError;
    use redis_async_pool::{RedisConnectionManager, RedisPool};
    
    pub fn get_connection_pool<T: AsRef<str>>(conn_params: T, connections_amount: usize) -> Result<RedisPool, RedisError> {
        let redis_conn = Client::open(conn_params.as_ref())?;
        let pool= RedisPool::new(
            RedisConnectionManager::new(redis_conn, true, None),
            connections_amount
        );
        Ok(pool)
    }
}

pub mod threadpool {
    use std::sync::{
        mpsc,
        Arc,
        Mutex
    };
    use log::*;
    use std::thread;
    use redis::streams::StreamMaxlen;
    use crate::redis_client::RedisClient;
    use serde_json::{Map, Value};

    type EventMap = Map<String, Value>;

    pub struct ThreadPool {
        workers: Vec<Worker>,
        sender: Mutex<mpsc::Sender<Message>>
    }

    enum Message {
        NewJob(EventMap),
        Terminate,
    }

    impl ThreadPool {
        pub fn new(size: usize, redis_client: RedisClient, stream_length: Option<usize>, stream_name: String)-> ThreadPool {
            assert!(size > 0);

            let (sender, receiver) = mpsc::channel();

            let receiver = Arc::new(Mutex::new(receiver));

            let mut workers = Vec::with_capacity(size);

            for _ in 0..size {
                workers.push(Worker::new(Arc::clone(&receiver), redis_client.clone(), stream_length, stream_name.clone()));
            }

            ThreadPool { workers, sender: Mutex::new(sender) }
        }

        pub fn execute(&self, event: EventMap)
        {
            self.sender.lock().unwrap().send(Message::NewJob(event)).unwrap();
        }
    } 

    impl Drop for ThreadPool {
        fn drop(&mut self) {
            for _ in &self.workers {
                self.sender.lock().unwrap().send(Message::Terminate).unwrap();
            }

            for worker in &mut self.workers {
                if let Some(thread) = worker.thread.take() {
                    thread.join().unwrap();
                }
            }
        }
    }

    struct Worker {
        thread: Option<thread::JoinHandle<()>>,
    }

    impl Worker {
        fn new(receiver: Arc<Mutex<mpsc::Receiver<Message>>>, redis_client: RedisClient, stream_length: Option<usize>, stream_name: String) -> Worker
        {
            info!("Starting worker!");
            let thread = thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                
                let opts = match stream_length {
                    None => None,
                    Some(v) => Some(StreamMaxlen::Approx(v))
                };

                loop {
                    let message = receiver.lock().unwrap().recv().unwrap();

                    if let Message::Terminate = message {
                        break
                    }

                    if let Message::NewJob(event) = message {
                        rt.block_on(async {
                            let res = redis_client.insert_into_stream(
                                stream_name.clone(),
                                serde_json::to_string(&event).unwrap(),
                                None,
                                opts
                            ).await;
                            if let Err(error) = res {
                                error!("Redis insert error: {:?}", error);
                            }
                        });
                    }
                }
            });

            Worker { thread: Some(thread) }
        }

    }
}

pub mod logger {
    use std::{env, io::Write};
    use chrono::Local;
    use log::info;

    pub fn init_logger(log_level: String) {
        env::set_var("RUST_LOG", log_level);
        env_logger::builder()
            .format_module_path(false)
            .format(
                |buf, record| {
                    writeln!(
                        buf,
                        "[{}:{} ] {}", Local::now().format("%d.%m.%Y %H:%M:%S.%s"), record.level(), record.args()
                    )
                }
            )
            .init();
        info!("Initialized logger...");
    }
}

pub mod server {
    use crate::threadpool::ThreadPool;
    use std::sync::Arc;
    use actix_web::{
        post, get, HttpResponse, HttpRequest, Responder,
        web::Data
    };
    use serde_json::Value;
    use log::*;

    #[derive(Clone)]
    pub struct HandlePool {
        pub pool: Box<Arc<ThreadPool>>
    }
    
    #[derive(Clone)]
    pub struct CallbackInfo {
        pub group_id: u64,
        pub secret: String,
        pub confirmation: String
    }

    #[post("/callback")]
    pub async fn callback(
        request: HttpRequest,
        req_body: String,
        callback: Data<CallbackInfo>,
        pool: Data<HandlePool>
    ) -> impl Responder {
        let bad_body_request: HttpResponse = HttpResponse::BadRequest().body("Bad request!");
        
        let headers = request.headers();
        if headers.contains_key("X-Retry-Counter") {
            warn!("Got repeat of message, skipping...");
            return HttpResponse::Ok().body("ok")
        }

        let event_data: Value = match serde_json::from_str(
            req_body.as_str()
        ) {
            Ok(v) => v,
            Err(error) => {
                error!("Body deserialisation failed! {:?}", error);
                return bad_body_request
            }
        };
        let event_data = match event_data.as_object() {
            Some(v) => v,
            None => {
                error!("Failed to deserialise body as object!");
                return bad_body_request
            }
        };
        
        if !event_data.contains_key("type") {
            debug!("Did not found type key in data!");
            return bad_body_request
        }

        if !event_data.contains_key("group_id") {
            debug!("Did not found group_id key in data!");
            return bad_body_request
        }

        let event_type: &str = match event_data["type"].as_str() {
            Some(v) => v,
            None => return bad_body_request
        };

        let group_id: u64 = match event_data["group_id"].as_u64() {
            Some(v) => v,
            None => return bad_body_request
        };

        if group_id != callback.group_id {
            error!("Invalid group id!");
            return HttpResponse::Unauthorized().body("Invalid group id!");
        }

        if event_type == "confirmation" {
            return HttpResponse::Ok().body(callback.confirmation.clone())
        }

        if !event_data.contains_key("secret") {
            debug!("Did not found secret key in data!");
            return bad_body_request
        }

        let secret = match event_data["secret"].as_str() {
            Some(value) => value,
            None => return bad_body_request
        };

        if secret != callback.secret {
            return HttpResponse::Unauthorized().body("Invalid secret key!");
        }

        if !event_data.contains_key("object") {
            debug!("Did not found object key in data!");
            return bad_body_request
        }

        match event_data["object"].as_object() {
            Some(v) => v,
            None => return bad_body_request
        };

        pool.pool.execute(event_data.clone());
        HttpResponse::Ok().body("ok")
    }

    #[get("/health")]
    pub async fn healthcheck() -> impl Responder {
        HttpResponse::Ok().body("ok")
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = config::Config::new("./config.toml").unwrap();
    logger::init_logger(config.get_variable("log_level").unwrap());

    let redis_client: redis_client::RedisClient = {
        let redis_url: String = config.get_variable("redis_url").unwrap();
        let pool = redis_pool::get_connection_pool(
            redis_url,
            config.get_variable("redis_connections").unwrap()
        ).unwrap();
        redis_client::RedisClient::new(pool)
    };

    let server_addr = {
        let server_port: String = config.get_variable("server_port").unwrap();
        let server_ip: String = config.get_variable("server_ip").unwrap();
        format!("{}:{}", server_ip, server_port)
    };
    info!("Working on {}", server_addr);

    let handlers_amount: i32 = config.get_variable("handlers_amount").unwrap();
    let workers_amount: i32 = config.get_variable("workers_amount").unwrap();
    let pool = threadpool::ThreadPool::new(
        handlers_amount as usize, 
        redis_client,
        config.get_variable("stream_length"),
        config.get_variable("stream_name").unwrap()
    );
    let pool_box = Box::new(Arc::new(pool));

    HttpServer::new(move || {
        let callback_info = server::CallbackInfo {
            group_id: config.get_variable("group_id").unwrap(),
            secret: config.get_variable("secret").unwrap(),
            confirmation: config.get_variable("confirmation").unwrap()
        };

        actix_web::App::new()
            .wrap(Logger::default())
            .service(server::callback)
            .service(server::healthcheck)
            .app_data(Data::new(server::HandlePool{ pool: pool_box.clone() }))
            .app_data(Data::new(callback_info))
    })
    .workers(workers_amount as usize)
    .bind(server_addr)?
    .run()
    .await
}
