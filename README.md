# flow-balancer
### A packet load balancer for flow-based Deep Packet Inspection in Go

## Flows
A Flow is defined by a packet's 5-tuple: Source IP, Dest IP, Source Port, Dest Port, and Transport Protocol ID. If two packets have the same 5-tuple, they are considered to be part of the same flow.

**Packets of the same Flow must be processed in the order they are received**. This is fundamental to the Deep Packet Inspection process.

## Load Balancing Algorithm
The `flow-balancer` uses a basic priority queue (heap) to load balance network packets across a number of worker goroutines. The goroutine that has the fewest number of pending requests will receive the next incoming packet, assuming that it is part of a yet-unprocessed Flow.

Once a packet from a new Flow is sent to a worker goroutine, all future packets belonging to this Flow must be sent to the same worker. Packets of the same Flow must be processed in the order they are received. The only way to guarantee this across a multi-threaded environment (without significant complexity) is to guarantee that the same worker processes all of the packets from a single Flow.

## Limitations

### Uneven Load Distribution
Because packets of the same Flow must be processed by the same worker, the load distribution is ultimately at the mercy of the 5-tuple distribution on the network being analyzed. If 90% of the network traffic shares a 5-tuple, 90% of the traffic will be processed by a single worker. 

There is a mechanism to alleviate this pressure. When a Flow is closed, either by timeout or by the Transport Protocol, the `balancer.Delete(hash)` function can be called. This will remove the 1-to-1 mapping from that 5-tuple hash and the worker that it is currently bound to. This way, a future Flow of the same hash can be processed by the least-loaded worker.

### Unsophisticated Calculation of Worker Load
The current calculation for which worker is the least-loaded is simply the worker with the shortest queue of packets to be processed. This is an admittedly poor metric for calculating a worker's load.

A better mechanism, in theory, would be the average processing time of packets through the worker. A worker that processes 10 packets in one second has a heavier load than a worker that processes 100 packets in 5 seconds. However, calculating time-based processing metrics is at the mercy of the OS scheduler, and for that reason, I believe that time-based metrics are largely unreliable.

In future work, I would like to make the load calculation based on a combination of worker backlog, as well as percentage of packets processed. For example, if Worker 1 and Worker 2 both have the same size backlog, but Worker 1 has processed 65% of the packets thus far, Worker 2 should receive this packet (this of course assumes that the incoming packet is the first packet in a new Flow). The reasoning would be that Worker 2 may be getting a new packet for a Flow that could balance out the distribution in the long run.
