# reconnect

Demo for using `BeforeConnect` options to change details of a PostgreSQL database connection every time the underlying pool needs to establish a new connection. Useful if you're using a database that allows dynamic credentials to be used to connect.

The demo also leverages GORM just to show that higher-level libraries work with this pattern.

## Run the test

```
docker compose up -d
go test .
```
