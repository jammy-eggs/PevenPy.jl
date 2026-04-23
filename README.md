# PevenPy.jl

`PevenPy.jl` is a small Julia adapter that lets Python drive the `Peven.jl`
engine over a framed MessagePack socket protocol.

Technically, it does four things:

1. Decodes authored env and run-start payloads sent from Python.
2. Lowers those payloads into `Peven.jl` nets, markings, guards, and join selectors.
3. Executes the loaded run with Python-backed transition executors by issuing
   callback requests over the same transport.
4. Streams engine lifecycle events and terminal run results back to Python in a
   normalized protocol shape.

The package is intentionally narrow. It is not a user-facing workflow layer and
it does not author nets itself. Its job is to be the Julia-side execution
boundary for the Python `peven` package.
