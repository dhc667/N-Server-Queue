from typing import Callable, Deque
from enum import Enum
import heapq

class Server:
    def __init__(self, service_time_generator: Callable[[], float], priority_generator: Callable[[], int]):
        self.service_time_generator = service_time_generator
        self.priority_generator = priority_generator

class EventType(Enum):
    Arrival = "arrival"
    Departure = "departure"

class EventData:
    def __init__(self, event_type: EventType, server_id: int | None = None):
        self.event_type = event_type
        self.server_id = server_id

    def __lt__(self, other):
        if self.event_type == EventType.Arrival:
            return True 
        else:
            return False

class Snapshot:
    def __init__(self, time: float, people_in_queue: int, people_serviced: list[int], free_servers: set[int], servers_serving: set[int]):
        self.time = time
        self.people_in_queue = people_in_queue
        self.people_serviced = people_serviced
        self.free_servers = free_servers
        self.servers_serving = servers_serving

    def __repr__(self):
        return f"Snapshot(time={self.time}, people_in_queue={self.people_in_queue}, people_serviced={self.people_serviced}, free_servers={self.free_servers}, servers_serving={self.servers_serving})"

    def __str__(self):
        return self.__repr__()

    def __lt__(self, other):
        return self.time < other.time

class NServerQueue:
    def __init__(self, servers: list[Server], arrival_generator: Callable[[], float], end_time: float):
        if (len(servers) == 0):
            raise ValueError("MGKQueue must have at least one server")

        self.servers = servers
        self.arrival_generator = arrival_generator
        self.end_time = end_time

        self.arrival_counter: int = 0
        self.current_time: float = 0
        self.event_queue: list[tuple[float, EventData]] = []
        self.people_in_queue = Deque[int]()
        self.servers_free: set[int] = set(range(len(servers)))
        self.servers_serving: list[int | None] = [None] * len(servers)
        self.people_serviced: list[int] = [0] * len(servers)
        self.captured_states: list[Snapshot] | None = None
        self.ran = False

        self.time_in_queue: dict[int, float] = {}
        self.time_in_service: dict[int, float] = {}
        self.arrival_time: dict[int, float] = {}

    def capture_state(self):
        servers_serving = set(
            filter(
                lambda x: self.servers_serving[x] is not None, 
                list(range(len(self.servers)))
            )
        )
        snapshot = Snapshot(self.current_time, len(self.people_in_queue), self.people_serviced.copy(), self.servers_free.copy(), servers_serving.copy())
        if self.captured_states == None:
            raise RuntimeError("expected captured states to not be None")
        self.captured_states.append(snapshot)

    def run(self, capture_states: bool = False, verbose: bool = False) :
        if self.ran:
            raise RuntimeError("model has already been run")

        self.event_queue.append((self.current_time + self.arrival_generator(), EventData(EventType.Arrival)))

        if not capture_states and verbose:
            raise ValueError("can only be verbose when capture_states is True")

        if capture_states:
            self.captured_states = []

        while len(self.event_queue) > 0:
            self.handle_event()
            if capture_states:
                self.capture_state()
            if verbose:
                if self.captured_states == None:
                    raise RuntimeError("expected captured states to not be None")
                print(self.captured_states[-1])

        self.ran = True

    def handle_event(self):
        self.current_time, event = heapq.heappop(self.event_queue)
        if event.event_type == EventType.Arrival:
            self.handle_arrival()
        elif event.event_type == EventType.Departure:
            if event.server_id is None:
                raise RuntimeError("expected server_id to not be None")
            self.handle_departure(event.server_id)
        else:
            raise RuntimeError(f"unknown event type {event}")


    def get_server_to_serve(self):
        server_id = min(self.servers_free, key=lambda x: self.servers[x].priority_generator())
        return server_id

    def handle_arrival(self):
        customer_id = self.arrival_counter
        self.arrival_time[customer_id] = self.current_time
        self.arrival_counter += 1
        if len(self.servers_free) > 0:
            server_id = self.get_server_to_serve()
            self.servers_free.remove(server_id)
            self.servers_serving[server_id] = customer_id
            self.time_in_queue[customer_id] = 0

            service_time = self.servers[server_id].service_time_generator()
            heapq.heappush(self.event_queue, (self.current_time + service_time, EventData(EventType.Departure, server_id)))
        else:
            self.people_in_queue.append(customer_id)


        if self.current_time < self.end_time:
            heapq.heappush(self.event_queue, (self.current_time + self.arrival_generator(), EventData(EventType.Arrival)))

    def handle_departure(self, server_id: int):
        customer_id = self.servers_serving[server_id]
        if customer_id is None:
            raise RuntimeError("expected customer_id to not be None")

        customer_service_time = self.current_time - self.arrival_time[customer_id] - self.time_in_queue[customer_id]
        self.time_in_service[customer_id] = customer_service_time

        self.people_serviced[server_id] += 1

        if len(self.people_in_queue) > 0:
            next_customer_id = self.people_in_queue.popleft()
            self.time_in_queue[next_customer_id] = self.current_time - self.arrival_time[next_customer_id]
            self.servers_serving[server_id] = next_customer_id

            service_time = self.servers[server_id].service_time_generator()
            heapq.heappush(self.event_queue, (self.current_time + service_time, EventData(EventType.Departure, server_id)))
        else:
            self.servers_free.add(server_id)
            self.servers_serving[server_id] = None



