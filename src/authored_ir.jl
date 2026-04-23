module AuthoredIR

using ..Protocol: ProtocolError

struct PlaceSpecMessage
    id::String
    capacity::Union{Nothing, Int}
    schema::Any
end

struct InputArcSpecMessage
    place::String
    weight::Int
end

struct OutputArcSpecMessage
    place::String
end

struct TransitionSpecMessage
    id::String
    executor::String
    inputs::Vector{InputArcSpecMessage}
    outputs::Vector{OutputArcSpecMessage}
    guard_spec::Any
    retries::Int
    join_by_spec::Any
end

struct EnvSpecMessage
    schema_version::Int
    env_name::String
    places::Vector{PlaceSpecMessage}
    transitions::Vector{TransitionSpecMessage}
end

struct LoadEnvMessage
    req_id::Int
    env::EnvSpecMessage
end

struct TokenMessage
    run_key::String
    color::String
    payload::Any
end

struct RunEnvMessage
    req_id::Int
    env_run_id::Int
    initial_marking::Dict{String,Vector{TokenMessage}}
    fuse::Union{Nothing,Int}
end

"""Decode one authored-IR load request from unpacked MessagePack data."""
function decode_load_env_message(value)
    message = _require_dict(value, "load_env message")
    _require_kind(message, "load_env")
    req_id = _require_positive_integer(message, "req_id")
    env = decode_env_spec_message(_require_key(message, "env"); context="load_env message")
    return LoadEnvMessage(req_id, env)
end

"""Decode one run-start request carrying an initial marking payload."""
function decode_run_env_message(value)
    message = _require_dict(value, "run_env message")
    _require_kind(message, "run_env")
    req_id = _require_positive_integer(message, "req_id")
    env_run_id = _require_positive_integer(message, "env_run_id")
    initial_marking = decode_initial_marking(
        _require_key(message, "initial_marking");
        context="run_env message",
    )
    fuse = _get_optional_integer(message, "fuse")
    isnothing(fuse) || fuse >= 0 || throw(ProtocolError("fuse must be non-negative when present"))
    return RunEnvMessage(req_id, env_run_id, initial_marking, fuse)
end

"""Decode one authored env payload from unpacked MessagePack data."""
function decode_env_spec_message(value; context::AbstractString="authored env")
    env = _require_dict(value, context)
    schema_version = _require_positive_integer(env, "schema_version")
    env_name = _require_nonempty_string(env, "env_name")
    places = _require_vector(env, "places")
    transitions = _require_vector(env, "transitions")
    return EnvSpecMessage(
        schema_version,
        env_name,
        [decode_place_spec_message(place) for place in places],
        [decode_transition_spec_message(transition) for transition in transitions],
    )
end

function decode_place_spec_message(value)
    place = _require_dict(value, "place spec")
    id = _require_nonempty_string(place, "id")
    capacity = _get_optional_integer(place, "capacity")
    capacity === nothing || capacity > 0 || throw(ProtocolError("place capacity must be positive when present"))
    schema = get(place, "schema", nothing)
    return PlaceSpecMessage(id, capacity, schema)
end

function decode_transition_spec_message(value)
    transition = _require_dict(value, "transition spec")
    id = _require_nonempty_string(transition, "id")
    executor = _require_nonempty_string(transition, "executor")
    inputs = _require_vector(transition, "inputs")
    outputs = _require_vector(transition, "outputs")
    retries = _get_optional_integer(transition, "retries")
    retries = retries === nothing ? 0 : retries
    retries >= 0 || throw(ProtocolError("transition retries must be non-negative"))
    return TransitionSpecMessage(
        id,
        executor,
        [decode_input_arc_spec_message(arc) for arc in inputs],
        [decode_output_arc_spec_message(arc) for arc in outputs],
        get(transition, "guard_spec", nothing),
        retries,
        get(transition, "join_by_spec", nothing),
    )
end

function decode_input_arc_spec_message(value)
    arc = _require_dict(value, "input arc spec")
    place = _require_nonempty_string(arc, "place")
    weight = _get_optional_integer(arc, "weight")
    weight = weight === nothing ? 1 : weight
    weight > 0 || throw(ProtocolError("input arc weight must be positive"))
    return InputArcSpecMessage(place, weight)
end

function decode_output_arc_spec_message(value)
    arc = _require_dict(value, "output arc spec")
    place = _require_nonempty_string(arc, "place")
    return OutputArcSpecMessage(place)
end

function decode_initial_marking(value; context::AbstractString="initial marking")
    marking = _require_dict(value, context)
    decoded = Dict{String,Vector{TokenMessage}}()
    sizehint!(decoded, length(marking))
    for (raw_place, raw_bucket) in pairs(marking)
        raw_place isa AbstractString || throw(ProtocolError("marking place ids must be strings"))
        place = String(raw_place)
        isempty(place) && throw(ProtocolError("marking place ids must be non-empty"))
        raw_bucket isa AbstractVector || throw(ProtocolError("marking buckets must be lists"))
        decoded[place] = [decode_token_message(token) for token in raw_bucket]
    end
    return decoded
end

function decode_token_message(value)
    token = _require_dict(value, "token")
    run_key = _require_nonempty_string(token, "run_key")
    color = _require_nonempty_string(token, "color")
    payload = get(token, "payload", nothing)
    return TokenMessage(run_key, color, payload)
end

function _require_kind(message::AbstractDict, expected::String)
    kind = _require_kind(message)
    String(kind) == expected || throw(ProtocolError("expected message kind $(repr(expected))"))
    return nothing
end

function _require_kind(message::AbstractDict)
    return _require_nonempty_string(message, "kind")
end

function _require_dict(value, context::AbstractString)
    value isa AbstractDict || throw(ProtocolError("$(context) must be a map"))
    return value
end

function _require_vector(message::AbstractDict, key::String)
    value = _require_key(message, key)
    value isa AbstractVector || throw(ProtocolError("$(key) must be a list"))
    return value
end

function _require_string_vector(message::AbstractDict, key::String)
    value = _require_vector(message, key)
    return [
        element isa AbstractString ? String(element) :
        throw(ProtocolError("$(key) must contain only strings"))
        for element in value
    ]
end

function _require_nonempty_string(message::AbstractDict, key::String)
    value = _require_key(message, key)
    value isa AbstractString || throw(ProtocolError("$(key) must be a string"))
    isempty(value) && throw(ProtocolError("$(key) must be non-empty"))
    return String(value)
end

function _require_positive_integer(message::AbstractDict, key::String)
    value = _require_key(message, key)
    value isa Integer || throw(ProtocolError("$(key) must be an integer"))
    value > 0 || throw(ProtocolError("$(key) must be positive"))
    return Int(value)
end

function _get_optional_integer(message::AbstractDict, key::String)
    value = get(message, key, nothing)
    value === nothing && return nothing
    value isa Integer || throw(ProtocolError("$(key) must be an integer when present"))
    return Int(value)
end

function _require_key(message::AbstractDict, key::String)
    haskey(message, key) || throw(ProtocolError("missing required field $(repr(key))"))
    return message[key]
end

function _require_dict_field(message::AbstractDict, key::String)
    value = _require_key(message, key)
    value isa AbstractDict || throw(ProtocolError("$(key) must be a map"))
    return value
end

end # module AuthoredIR
