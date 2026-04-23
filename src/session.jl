module Session

using Sockets

import ..Adapter
using ..Adapter:
    AdapterState,
    accept_message!,
    execute_loaded_run!,
    exchange_callback!,
    emit_message!
using ..Protocol
using ..Protocol: ProtocolError, decode_message, handshake_frame, read_frame, write_frame

mutable struct IOSessionTransport
    io::IO
    write_lock::ReentrantLock
    pending_lock::ReentrantLock
    pending_replies::Dict{Int,Channel{Any}}
    poisoned::Bool
    fail_event_kind::Union{Nothing,String}
end

IOSessionTransport(io::IO; fail_event_kind::Union{Nothing,String}=nothing) =
    IOSessionTransport(
        io,
        ReentrantLock(),
        ReentrantLock(),
        Dict{Int,Channel{Any}}(),
        false,
        fail_event_kind,
    )

"""Accept one Python bootstrap connection and serve one framed adapter session."""
function serve(socket_path::AbstractString)
    return _serve_socket(socket_path; fail_event_kind=nothing)
end

"""Test-only session entrypoint with injectable runtime event failures."""
function serve_testonly(socket_path::AbstractString; fail_event_kind::Union{Nothing,String}=nothing)
    return _serve_socket(socket_path; fail_event_kind=fail_event_kind)
end

function _serve_socket(socket_path::AbstractString; fail_event_kind::Union{Nothing,String}=nothing)
    server = listen(socket_path)
    client = nothing
    try
        client = accept(server)
        write(client, handshake_frame())
        flush(client)
        _serve_session(client; fail_event_kind=fail_event_kind)
    finally
        client === nothing || _close_quietly(client)
        _close_quietly(server)
    end
    return nothing
end

function _serve_session(io::IO; fail_event_kind::Union{Nothing,String}=nothing)
    state = AdapterState()
    transport = IOSessionTransport(io; fail_event_kind=fail_event_kind)
    incoming = Channel{Any}(Inf)
    reader = @async _reader_loop(transport, incoming)
    try
        while true
            message = try
                take!(incoming)
            catch
                return nothing
            end
            reply = nothing
            accepted_run = nothing
            try
                accepted = accept_message!(state, message)
                reply = accepted.reply
                accepted_run = accepted.accepted_run
            catch error
                error isa ProtocolError || rethrow()
                return nothing
            end
            try
                lock(transport.write_lock) do
                    write_frame(transport.io, Protocol.encode_message(reply))
                end
            catch error
                error isa InterruptException && rethrow()
                return nothing
            end
            if accepted_run !== nothing
                try
                    execute_loaded_run!(state, accepted_run, transport)
                catch error
                    error isa ProtocolError || rethrow()
                    return nothing
                end
            end
        end
    finally
        Adapter.poison_transport!(transport)
        try
            wait(reader)
        catch
        end
    end
end

function _reader_loop(transport::IOSessionTransport, incoming::Channel{Any})
    try
        while true
            payload = try
                read_frame(transport.io)
            catch
                return nothing
            end
            payload === nothing && return nothing
            message = try
                decode_message(payload)
            catch
                return nothing
            end
            kind = message isa AbstractDict ? get(message, "kind", nothing) : nothing
            req_id_value = message isa AbstractDict ? get(message, "req_id", nothing) : nothing
            if (kind == "callback_reply" || kind == "callback_error") && req_id_value isa Integer
                ch = lock(transport.pending_lock) do
                    pop!(transport.pending_replies, Int(req_id_value), nothing)
                end
                ch === nothing && return nothing
                put!(ch, message)
                continue
            end
            put!(incoming, message)
        end
    finally
        close(incoming)
        lock(transport.pending_lock) do
            for ch in values(transport.pending_replies)
                close(ch)
            end
            empty!(transport.pending_replies)
        end
    end
end

function Adapter.emit_message!(transport::IOSessionTransport, message)
    lock(transport.write_lock)
    try
        transport.poisoned && throw(ProtocolError("transport is poisoned"))
        if !isnothing(transport.fail_event_kind) &&
           get(message, "kind", nothing) == transport.fail_event_kind
            throw(ProtocolError(
                "injected runtime event failure for kind $(repr(transport.fail_event_kind))",
            ))
        end
        write_frame(transport.io, Protocol.encode_message(message))
    catch error
        error isa InterruptException && rethrow()
        Adapter.poison_transport!(transport)
        error isa ProtocolError && rethrow()
        throw(ProtocolError("failed to emit runtime message: $(sprint(showerror, error))"))
    finally
        unlock(transport.write_lock)
    end
    return nothing
end

function Adapter.exchange_callback!(transport::IOSessionTransport, request)
    req_id = Int(request["req_id"])
    channel = Channel{Any}(1)
    lock(transport.pending_lock) do
        transport.pending_replies[req_id] = channel
    end
    try
        lock(transport.write_lock) do
            transport.poisoned && throw(ProtocolError("transport is poisoned"))
            write_frame(transport.io, Protocol.encode_message(request))
        end
        reply = try
            take!(channel)
        catch
            throw(ProtocolError("session closed while waiting for callback reply"))
        end
        return reply
    catch error
        lock(transport.pending_lock) do
            delete!(transport.pending_replies, req_id)
        end
        error isa InterruptException && rethrow()
        Adapter.poison_transport!(transport)
        error isa ProtocolError && rethrow()
        throw(ProtocolError("callback exchange failed: $(sprint(showerror, error))"))
    end
end

function Adapter.poison_transport!(transport::IOSessionTransport)
    transport.poisoned = true
    _close_quietly(transport.io)
    return nothing
end

function _close_quietly(io)
    try
        close(io)
    catch
        nothing
    end
    return nothing
end

end # module Session
