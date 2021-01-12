# SEDA Bus
<div align="center">
  <img src="https://resolvingarchitecture.io/images/ra.png"  />

  <h1>Resolving Architecture</h1>

  <p>
    <strong>Clarity in Design</strong>
  </p>

<h2>SEDA Bus</h2>

  <p>
   Staged Event-Driven Architecture Bus - A form of message bus avoiding the high overhead of thread-based concurrency models where channels get their own inbound and outbound queues.
  </p>

  <p>
    <a href="https://travis-ci.com/resolvingarchitecture/seda-bus-java"><img alt="build" src="https://img.shields.io/travis/resolvingarchitecture/seda-bus-java"/></a>
    <a href="https://resolvingarchitecture.io/ks/publickey.brian@resolvingarchitecture.io.asc"><img alt="PGP" src="https://img.shields.io/keybase/pgp/objectorange"/></a>
    <img alt="repo size" src="https://img.shields.io/github/repo-size/resolvingarchitecture/seda-bus-java"/>
  </p>
  <p>
    <img alt="num lang" src="https://img.shields.io/github/languages/count/resolvingarchitecture/seda-bus-java"/>
    <img alt="top lang" src="https://img.shields.io/github/languages/top/resolvingarchitecture/seda-bus-java"/>
  </p>

  <h4>
    <a href="https://resolvingarchitecture.io">Site</a>
    <span> | </span>
    <a href="https://docs.rs/crate/seda-bus/">Docs</a>
    <span> | </span>
    <a href="https://github.com/resolvingarchitecture/seda-bus-java/blob/master/CHANGELOG.md">Changelog</a>
  </h4>
</div>

## Donate
Request BTC address for a donation at brian@resolvingarchitecture.io.

## Notes


## Roadmap

*[x] 1.0.0 - Minimal Stable Useful Functionality
*[ ] 2.0.0 - Support [dbus](https://en.wikipedia.org/wiki/D-Bus) for inter-process communications on Linux

## Summary
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

This component is also implemented in [Rust](https://github.com/resolvingarchitecture/seda-bus) and [Typescript](https://github.com/resolvingarchitecture/seda-bus-ts).
This project was the original SEDA Bus implementation for Resolving Architecture.
