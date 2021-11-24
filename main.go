package main

import (
    "flag"
    "net/http"
    "log"
    "context"
    "github.com/gorilla/websocket"
    "github.com/gorilla/mux"
    "github.com/go-redis/redis/v8"
)

var addr = flag.String("addr", "0.0.0.0:8080", "http server address")
var redis_host = flag.String("redis_host", "0.0.0.0:6379", "redis host address")
var redis_pass = flag.String("redis_pass", "", "redis password")

var upgrader = websocket.Upgrader{}
var rdb *redis.Client

// this method reads from the websocket and publishes to redis
func PublishHandler(c *websocket.Conn, channel string) {
    for {
        // publish received message to channel
        _, message, err := c.ReadMessage()
        if err != nil {
            log.Println("error reading:", err)
            break
        }

        rdb.Publish(context.Background(), channel, message)
    }
}

// this methods subscribes to redis and writes to the websocket
func SubscribeHandler(c *websocket.Conn, channel string) {
    pubsub := rdb.Subscribe(context.Background(), channel)
    ch := pubsub.Channel()

    for msg := range ch {
        c.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
    }
}

func RequestHandler(w http.ResponseWriter, r *http.Request) {
    // make the request a websocket
    c, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        log.Print("upgrade:", err)
		return
    }

    // defer c.Close()
  
    channel := mux.Vars(r)["channel"]

    go PublishHandler(c, channel)
    go SubscribeHandler(c, channel)
}

func main() {
    flag.Parse()

    // connect to redis
    rdb = redis.NewClient(&redis.Options{
        Addr: *redis_host,
        Password: *redis_pass,
        DB: 0,
    })

    // create http server
    r := mux.NewRouter()
    r.HandleFunc("/{channel}", RequestHandler)
    http.Handle("/", r)

    log.Fatal(http.ListenAndServe(*addr, nil))
}
