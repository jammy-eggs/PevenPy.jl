module PevenPy

include("protocol.jl")
include("authored_ir.jl")
include("lowering.jl")
include("adapter.jl")
include("session.jl")

using .Protocol: HANDSHAKE_TAG, PEVEN_VERSION, PROTOCOL_VERSION
using .Session: serve, serve_testonly

export HANDSHAKE_TAG, PEVEN_VERSION, PROTOCOL_VERSION, main

"""Run one adapter process against the Unix socket path from Python bootstrap."""
function main(socket_path::AbstractString)
    serve(socket_path)
    return nothing
end

"""Internal test-only entrypoint for adapter integration failure injection."""
function _test_main(
    socket_path::AbstractString;
    fail_event_kind::Union{Nothing,String}=nothing,
)
    serve_testonly(socket_path; fail_event_kind=fail_event_kind)
    return nothing
end

end # module PevenPy
