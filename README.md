# RediSetGo

RediSetGo is a simple Redis-like key-value store implemented in Go. It supports basic Redis commands such as `SET`, `GET`, `HSET`, `HGET`, and more. This project is intended for educational purposes to understand the internals of Redis and how it handles various commands and data structures in addition to building some working experience with Go.

## Features

- Basic Redis commands: `SET`, `GET`, `DEL`, `HSET`, `HGET`, `HGETALL`, `HDEL`, `PING`
- Append-only file for persistence
- Append-only file shrunk on startup removing deleted items and delete commands(CURRENTLY BROKEN, need to resolve temp file error)
- Simple in-memory data structures

## Getting Started

### Prerequisites

- Go 1.16 or later
- [Redis client](https://redis.io/docs/getting-started/installation/)

### Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/yourusername/RediSetGo.git
    cd RediSetGo
    ```

2. Build the project:

    ```sh
    go build
    ```

3. Run the project:

    ```sh
    ./RediSetGo
    ```

    If after installing the Redis Client you're getting Listener Creation errors because the address, 6379, is already in use you will need to kill the redis server currently running.

    ```sh
    sudo snap stop redis
    ```
    or
    ```sh
    sudo kill redis-server
    ```

## Usage

### Supported Commands

- **SET**: Sets a key to hold a value

    ```sh
    SET key value
    ```

- **GET**: Gets the value of a key

    ```sh
    GET key
    ```

- **DEL**: Deletes the key/value pair OR hashmap specified
    ```sh
    DEL key
    ```

- **HSET**: Sets a field in a hash

    ```sh
    HSET hash field value
    ```

- **HGET**: Gets the value of a field in a hash

    ```sh
    HGET hash field
    ```

- **HGETALL**: Gets value for all fields in a hash

    ```sh
    HGETALL hash
    ```

- **HDEL**: Delete the value of a field in a hash

    ```sh
    HDEL hash field
    ```

- **HGETALL**: Gets all the fields and values in a hash

    ```sh
    HGETALL hash
    ```

- **KEYS**: Gets all keys that match a pattern

    ```sh
    KEYS pattern
    ```

- **PING**: Simple test command to check server availability

    ```sh
    PING
    ```

### Example Usage

To set a key and then retrieve it:

```sh
SET mykey myvalue
GET mykey
```
To work with hashes:

```sh
HSET myhash field1 value1
HGET myhash field1
HGETALL myhash
```

## Acknowledgements
This project was inspired partially by the first few modules I completed of CodeCrafters' ["Build Your Own Redis" challenge](https://app.codecrafters.io/courses/redis) and a [tutorial](https://www.build-redis-from-scratch.dev/en/introduction) I subsequently found and followed by Ahmed Ashraf.
