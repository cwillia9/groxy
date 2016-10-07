# groxy
Not production ready!

Things that still need to be done:
* Tests...I haven't even really finalized the API, so tests are a bit hard
* Handling error conditions (If the kafka broker that is the leader dies, does the code handle that?)
* Sane logging

## What is this?
Groxy is intended to act as a proxy that will decouple the handling of an HTTP request from the
business logic of what to do with the data that is actually in the request. Ok, high level view...

lifecycle:
1. request comes from client
2. load balancer directs request to groxy
3. groxy serializes request and pushes onto kafka
4. kafka consumer (what would normally be your web server) pulls request and processes it
5. response is pushed onto kafka
6. groxy picks up request and sends it back to load balancer
7. back to client

benefits:
1. easier fine tuning of resources
2. web servers could be designed from the ground up to handle batches of requests at a time
3. auxiliary services can process the same traffic the main service is handling, for example real-time fraud detection
4. when deploying a new version of a service, you can qa with real-time production traffic and compare the responses and performance
 
