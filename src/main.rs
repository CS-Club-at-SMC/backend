use std::{collections::HashMap, env::vars};
use actix::{Actor, StreamHandler};
use actix_web::{web, HttpRequest, Responder, HttpResponse, HttpServer, App, middleware::{DefaultHeaders, Compress}};
use actix_web_actors::ws;
use chrono::prelude::*;
use dgraph_tonic::{Client, Mutate, Mutation, Operation, Query};
use maplit::hashmap;
extern crate qstring;
use qstring::QString;
use serde::{Deserialize, Serialize};

/// Drops all data in the dgraph database and wipes the schema
async fn drop_all(client: &Client) {
    let op = Operation {
        drop_all: true,
        ..Default::default()
    };
    client.alter(op).await.expect("dropped all");
}

/// Sets the schema for this application
async fn set_schema(client: &Client) {
    let schema = r#"
        name: string @index(exact) .
        age: int .
    "#
    .into();
    let op = Operation {
        schema,
        ..Default::default()
    };
    client.alter(op).await.expect("set schema");
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Location {
    #[serde(rename = "type", alias = "type")]
    t: String,
    coordinates: Vec<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Friend {
    uid: String,
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct School {
    name: String,
    schooltype: SchoolType,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum SchoolType {
    Elementary,
    Middle,
    High,
    College,
    University,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Discord {
    uid: String,
    handle: Option<String>,
    display_name: Option<String>,
    user_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Instagram {
    handle: Option<String>,
    display_name: Option<String>,
    user_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct X {
    handle: Option<String>,
    display_name: Option<String>,
    user_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct All {
    all: Vec<Person>
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Person {
    uid: Option<String>,
    name: Option<String>,
    email: Option<String>,
    discord: Option<Discord>,
    instagram: Option<Instagram>,
    snapchat: Option<String>,
    x: Option<X>,
    school: Option<Vec<School>>,
    misc: Option<Vec<String>>,
}

impl Person {
    fn new(
        uid: String,
        name: Option<String>,
        email: Option<String>,
        discord: Option<Discord>,
        instagram: Option<Instagram>,
        snapchat: Option<String>,
        x: Option<X>,
        school: Option<Vec<School>>,
        misc: Option<Vec<String>>,
    ) -> Self {
        Person {
            uid: Some(uid),
            name,
            email,
            discord,
            instagram,
            snapchat,
            x,
            school,
            misc,
        }
    }
    async fn new_node(client: &Client, name: String) -> String {
        let p = Person {
            uid: format!("_:{}", name.to_ascii_lowercase()).into(),
            name: Some(name),
            email: None,
            discord: None,
            instagram: None,
            snapchat: None,
            x: None,
            school: None,
            misc: None,
        };
        if let Ok(json_string) = serde_json::to_string(&p) {
            println!("{}", json_string);
        } else {
            println!("Failed to serialize to JSON.");
        }
        return create_data(client, p).await;
    }
    async fn update_person<F>(client: &Client, name: String, updater: F) -> String where F: FnOnce(&mut Person), {
        let query = r#"
            query all($a: string) {
                all(func: eq(name, $a)) {
                    uid
                    name
                    email
                    discord {
                        uid
                        handle
                        display_name
                        user_id
                    }
                    instagram {
                        handle
                        display_name
                        user_id
                    }
                    snapchat
                    x {
                        handle
                        display_name
                        user_id
                    }
                    school {
                        name
                        schooltype
                    }
                    misc
                }
            }
        "#;
        let vars = hashmap! {"$a" => name.clone()};
        println!("{}", name);
        let resp = client
            .new_read_only_txn()
            .query_with_vars(query, vars)
            .await
            .expect("resp");
        let ppl: All = serde_json::from_slice::<All>(&resp.json).expect("Failed to deserialize binary data");
        println!("{:#?}", ppl);
        let mut person = ppl.all.first().unwrap().to_owned();
        return create_data(client, person).await;
    }
}

trait HasUid {
    fn get_uid(&self) -> String;
    fn get_name(&self) -> String;
}

impl HasUid for Person {
    fn get_uid(&self) -> String {
        <std::option::Option<std::string::String> as Clone>::clone(&self.uid).unwrap()
    }
    fn get_name(&self) -> String {
        self.name.as_ref().unwrap().to_lowercase().to_string()
    }
}

async fn create_data<T>(client: &Client, data: T) -> String
where
    T: serde::Serialize + std::fmt::Debug + HasUid,
{
    let uid = data.get_name();
    let mut txn = client.new_mutated_txn();
    let mut mu = Mutation::new();
    mu.set_set_json(&data).expect("JSON");
    let response = txn.mutate(mu).await.expect("mutated");
    txn.commit().await.expect("committed");
    

    println!("{:#?}, {}", response.uids, uid);
    let id = response
        .uids
        .get(uid.as_str())
        .to_owned();
    if id.is_some() {
        return id.unwrap().to_string();
    } else {
        return data.get_uid();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Nodes {
    #[serde(rename = "queryNodes")]
    query_nodes: Vec<Node>
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Node {
    uid: String,
    name: String,
}


async fn query_nodes(client: &Client) -> HashMap<String, String> {
    let query = r#"
        query {
            queryNodes(func: has(name)) {
                uid
                name
            }
        }      
    "#;
    let resp = client
        .new_read_only_txn()
        .query(query)
        .await
        .expect("resp");
    let nodes: Nodes = serde_json::from_slice::<Nodes>(&resp.json).expect("Failed to deserialize binary data");
    
    let mut ret_map = HashMap::new();
    for node in nodes.query_nodes {
        ret_map.insert(node.name, node.uid);
    }
    println!("{:#?}", ret_map);
    return ret_map;
}

pub struct FriendWs {
    user: String,
}

impl Actor for FriendWs {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        return ctx.text(format!("Hello!\n"));
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for FriendWs {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                ctx.text(text);
            },
            _ => (),
        }
    }
}

async fn friendws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, actix_web::Error> {
    let qs = QString::from(req.query_string());
    let user = match qs.get("user") {
        Some(user) => user.to_string(),
        None => {
            return Ok(HttpResponse::NotFound()
                .insert_header(("Content-Type", "text/plain"))
                .body("Error: No user specified\n"))
        }
    };
    let resp = ws::start(
        FriendWs {
            user,
        },
        &req,
        stream,
    );
    println!("{:?}", resp);
    resp
}

/// The index of the website
async fn index(_req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body("This is the backend for the website")
}

/// Endpoint for getting a name's uid
async fn getuid(req: HttpRequest) -> impl Responder {
    let client = Client::new(vec!["http://localhost:9080"]).expect("connected client");
    let qs = QString::from(req.query_string());
    let name = qs.get("user");
    if name.is_none() {
        return HttpResponse::BadRequest().body("user url_param cannot be empty");
    }
    let name = name.unwrap().to_string();
    let query = r#"
        query all($a: string) {
            all(func: eq(name, $a)) {
                uid
                name
            }
        }
    "#;
    let vars = hashmap! { "$a" => name };
    let resp = client
        .new_read_only_txn()
        .query_with_vars(query, vars)
        .await
        .expect("resp");
    let ppl: All = serde_json::from_slice::<All>(&resp.json).expect("Failed to deserialize binary data");
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body(ppl.all.first().unwrap().uid.clone().unwrap())
}

/// Endpoint for adding a user
async fn adduser(json_data: web::Json<Person>) -> impl Responder {
    let client = Client::new(vec!["http://localhost:9080"]).expect("connected client");
    println!("Received data: {:?}", json_data);
    let mut person = json_data.0;
    if person.name.is_none() {
        return HttpResponse::BadRequest().body("Name cannot be empty");
    }
    if person.uid.is_none() {
        person.uid = Some(format!("_:{}", person.name.clone().unwrap()));
    }
    let uid = create_data(&client, person).await;
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body(uid)
}

/// Endpoint for searching for a user or getting all users
async fn getusers(req: HttpRequest) -> impl Responder {
    let query = r#"
        query all($a: string) {
            all(func: eq(name, $a)) {
                uid
                name
                email
                discord {
                    uid
                    handle
                    display_name
                    user_id
                }
                instagram {
                    handle
                    display_name
                    user_id
                }
                snapchat
                school {
                    name
                    schooltype
                }
                misc
            }
        }
    "#;
    let query_all = r#"
        query all {
            all(func: has(name)) {
            uid
            name
            email
            discord {
                uid
                handle
                display_name
                user_id
            }
            instagram {
                handle
                display_name
                user_id
            }
            snapchat
            school {
                name
                schooltype
            }
            misc
            }
        }
    "#;
    let qs = QString::from(req.query_string());
    let client = Client::new(vec!["http://localhost:9080"]).expect("connected client");
    let resp = match qs.get("user") {
        Some(name) => {
            let vars = hashmap! { "$a" => name };
            client
                .new_read_only_txn()
                .query_with_vars(query, vars)
                .await
                .expect("resp")
        },
        None => {
            client
                .new_read_only_txn()
                .query(query_all)
                .await
                .expect("resp")
        }
    };
    let ppl: All = serde_json::from_slice::<All>(&resp.json).expect("Failed to deserialize binary data");
    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .body(serde_json::to_string(&ppl.all).expect("Failed to serialize to JSON string"))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let client = Client::new(vec!["http://localhost:9080"]).expect("connected client");
    let mut uid_map = query_nodes(&client).await;
    drop_all(&client).await;
    set_schema(&client).await;
    let x = Person::new_node(&client, "Joshua".to_owned()).await;
    uid_map.insert("Joshua".to_string(), x);
    let x = Person::new_node(&client, "Pronsh".to_owned()).await;
    uid_map.insert("Pronsh".to_string(), x);
    let x = Person::new_node(&client, "Justin".to_owned()).await;
    uid_map.insert("Justin".to_string(), x);
    //query_data(&client).await;
    println!("{:#?}", uid_map);
    println!("DONE!");

    let builder = HttpServer::new(|| {
        App::new()
            .wrap(DefaultHeaders::new().add(("Server", "Friends")).add(("Access-Control-Allow-Origin", "*")))
            .wrap(Compress::default())
            .route("/", web::get().to(index))
            .route("/getusers", web::get().to(getusers))
            .route("/adduser", web::post().to(adduser))
            .route("/getuid", web::get().to(getuid))
            .route("/friendws", web::get().to(friendws))
    })
    .workers(4);
    println!("Running on port: {}", 8081);
    builder.bind(format!("127.0.0.1:{}", 8081)).unwrap().run().await.unwrap();
    Ok(())
}
