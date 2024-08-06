# Swift Distributed Actor System Challenges

This repository contains implementations of the Distributed Systems Challenges 
from [Fly.io](https://fly.io/dist-sys/) in Swift 6.

It's based on a custom [Distributed Actor System](https://developer.apple.com/documentation/distributed/distributedactorsystem) developed whilst working on the challenges.
The project uses the new Swift 6 language mode and other features newly added to the language 
(Atomic, Mutex, explicit Ownership, Swift Testing, ...).

## Goals

The primary goal of this repository is to learn and showcase how [Distributed Actors](https://www.swift.org/blog/distributed-actors/) work. 
I might even be able to create tutorials on the topic whilst learning about it myself.

As every challenge will hopefully build on the previous one, I'd love to end up with a solid and well tested 
distributed actor system I can use for (peer-to-peer) multiplayer games.

## Challenges

Every finished challenge comes with a standalone implementation and tests to verify the results.

- [x] **[Challenge #1: Echo](https://fly.io/dist-sys/1/)** implemented in [`Sources/EchoSystem`](./Sources/EchoSystem), tested in [`Tests/EchoSystemTests`](./Tests/EchoSystemTests)
- [ ] **[Challenge #2: Unique ID Generator](https://fly.io/dist-sys/2/)**
- [ ] **[Challenge #3a: Single-Node Broadcast](https://fly.io/dist-sys/3a/)**
- [ ] **[Challenge #3b: Multi-Node Broadcast](https://fly.io/dist-sys/3b/)**
- [ ] **[Challenge #3c: Fault Tolerant Broadcast](https://fly.io/dist-sys/3c/)**
- [ ] **[Challenge #3d: Efficient Broadcast, Part I](https://fly.io/dist-sys/3d/)**
- [ ] **[Challenge #3e: Efficient Broadcast, Part II](https://fly.io/dist-sys/3e/)**
- [ ] **[Challenge #4: Grow-Only Counter](https://fly.io/dist-sys/4/)**
- [ ] **[Challenge #5a: Single-Node Kafka-Style Log](https://fly.io/dist-sys/5a/)**
- [ ] **[Challenge #5b: Multi-Node Kafka-Style Log](https://fly.io/dist-sys/5b/)**
- [ ] **[Challenge #5c: Efficient Kafka-Style Log](https://fly.io/dist-sys/5c/)**
- [ ] **[Challenge #6a: Single-Node, Totally-Available Transactions](https://fly.io/dist-sys/6a/)**
- [ ] **[Challenge #6b: Totally-Available, Read Uncommitted Transactions](https://fly.io/dist-sys/6b/)**
- [ ] **[Challenge #6c: Totally-Available, Read Committed Transactions](https://fly.io/dist-sys/6c/)**
