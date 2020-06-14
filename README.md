# SEDA Bus
Staged Event-Driven Architecture (SEDA) is an approach to software architecture that decomposes a complex,
event-driven application into a set of stages connected by queues. It avoids the high overhead associated
with thread-based concurrency models (i.e. locking, unlocking, and polling for locks), and decouples event
and thread scheduling from application logic. By performing admission control on each event queue, the
service can be well-conditioned to load, preventing resources from being over-committed when demand exceeds
service capacity.

SEDA employs dynamic control to automatically tune runtime parameters (such as the scheduling parameters of
each stage) as well as to manage load (like performing adaptive load shedding). Decomposing services into a
set of stages also enables modularity and code reuse, as well as the development of debugging tools for
complex event-driven applications.

A Bus type architectural router style is decentralized in nature such that each instance of a node can be used
with other bus nodes, messages need to go through any specific node as in more centralized routers like
hub-and-spoke type routers. This is accomplished by supporting publishers and consumers using their own addressing
schemes creating their own mappings through chosen message channels, e.g. one message channel could be associated
with a persistence type service while another with a network type service - the bus cares not which is called
they're all just endpoints.

Bringing together SEDA and Bus architectural patterns is what this component attempts.