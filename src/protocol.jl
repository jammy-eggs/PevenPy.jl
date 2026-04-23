module Protocol

using MsgPack

const HANDSHAKE_TAG = "peven-runtime-handshake"
const PROTOCOL_VERSION = "0.1.0"
const PEVEN_VERSION = "0.2.0"
const MAX_FRAME_BYTES = 8 * 1024 * 1024

"""Message-level protocol error for malformed or unsupported adapter payloads."""
struct ProtocolError <: Exception
    message::String
end

Base.showerror(io::IO, error::ProtocolError) = print(io, error.message)

"""Return the startup handshake map expected by Python bootstrap."""
function handshake_message()
    return Dict(
        "tag" => HANDSHAKE_TAG,
        "protocol_version" => PROTOCOL_VERSION,
        "peven_version" => PEVEN_VERSION,
    )
end

"""Encode one Julia value as MessagePack bytes."""
encode_message(value) = MsgPack.pack(value)

"""Decode one MessagePack payload into plain Julia containers."""
decode_message(payload::Vector{UInt8}) = MsgPack.unpack(payload)

"""Prefix one payload with its 4-byte big-endian length."""
function encode_frame(payload::Vector{UInt8})
    length(payload) <= MAX_FRAME_BYTES ||
        throw(ProtocolError("frame size $(length(payload)) exceeds max frame size $(MAX_FRAME_BYTES)"))
    io = IOBuffer()
    write(io, hton(UInt32(length(payload))))
    write(io, payload)
    return take!(io)
end

"""Read one framed payload or return `nothing` on clean EOF before the next frame."""
function read_frame(io::IO)
    frame_length = nothing
    try
        frame_length = ntoh(read(io, UInt32))
    catch error
        error isa EOFError && return nothing
        rethrow()
    end
    frame_length <= MAX_FRAME_BYTES ||
        throw(ProtocolError("frame size $(frame_length) exceeds max frame size $(MAX_FRAME_BYTES)"))
    return read(io, frame_length::UInt32)
end

"""Write exactly one framed payload to the peer."""
function write_frame(io::IO, payload::Vector{UInt8})
    write(io, encode_frame(payload))
    flush(io)
    return nothing
end

"""Return the framed startup handshake expected by Python bootstrap."""
handshake_frame() = encode_frame(encode_message(handshake_message()))

end # module Protocol
