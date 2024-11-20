use actix_web::{web, App, HttpServer, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::env;

#[derive(Debug, Serialize, Deserialize)]
struct Counter {
    count: i64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

// Handler for POST requests
async fn update_counter(
    path: web::Path<(String, String)>,
    counter: web::Json<Counter>,
    db: web::Data<PgPool>,
) -> Result<HttpResponse, actix_web::Error> {
    let (namespace, counter_name) = path.into_inner();

    // Upsert the counter value using PostgreSQL's ON CONFLICT syntax
    let result = sqlx::query!(
        r#"
        INSERT INTO counters (namespace, counter_name, count)
        VALUES ($1, $2, $3)
        ON CONFLICT (namespace, counter_name)
        DO UPDATE SET count = EXCLUDED.count
        "#,
        namespace,
        counter_name,
        counter.count
    )
        .execute(db.get_ref())
        .await;

    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(counter.into_inner())),
        Err(e) => {
            let error_response = ErrorResponse {
                error: e.to_string(),
            };
            Ok(HttpResponse::InternalServerError().json(error_response))
        }
    }
}

// Handler for GET requests
async fn get_counter(
    path: web::Path<(String, String)>,
    db: web::Data<PgPool>,
) -> Result<HttpResponse, actix_web::Error> {
    let (namespace, counter_name) = path.into_inner();

    let result = sqlx::query!(
        r#"
        SELECT count FROM counters
        WHERE namespace = $1 AND counter_name = $2
        "#,
        namespace,
        counter_name
    )
        .fetch_optional(db.get_ref())
        .await;

    match result {
        Ok(Some(row)) => Ok(HttpResponse::Ok().json(Counter { count: row.count.unwrap_or(0) })),
        Ok(None) => Ok(HttpResponse::NotFound().json(ErrorResponse {
            error: "Counter not found".to_string(),
        })),
        Err(e) => Ok(HttpResponse::InternalServerError().json(ErrorResponse {
            error: e.to_string(),
        })),
    }
}

// Initialize database by creating the table if it doesn't exist
async fn init_db(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        CREATE TABLE IF NOT EXISTS counters (
            namespace TEXT,
            counter_name TEXT,
            count BIGINT,
            PRIMARY KEY (namespace, counter_name)
        )
        "#
    )
        .execute(pool)
        .await?;

    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load environment variables from .env file if present
    dotenv::dotenv().ok();

    // Get the database URL from environment variable
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let api_port = env::var("PORT")
        .expect("PORT must be set");

    // Set up the database connection pool
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to create pool");

    // Initialize the database
    init_db(&pool)
        .await
        .expect("Failed to initialize database");

    // Create the web server
    println!("Starting server at http://127.0.0.1:{api_port}");
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .route("/api/{namespace}/{counter}", web::post().to(update_counter))
            .route("/api/{namespace}/{counter}", web::get().to(get_counter))
    })
        .bind(format!("127.0.0.1:{api_port}"))?
        .run()
        .await
}