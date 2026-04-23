module Lowering

using Peven

using ..AuthoredIR:
    EnvSpecMessage,
    InputArcSpecMessage,
    OutputArcSpecMessage,
    PlaceSpecMessage,
    TokenMessage,
    TransitionSpecMessage,
    _require_dict_field,
    _require_kind,
    _require_nonempty_string,
    _require_string_vector,
    _require_vector
using ..Protocol: ProtocolError

struct LoweredEnv
    net::Peven.Net
end

"""Lower one authored env payload into exported `Peven` values."""
function lower_env(authored_env::EnvSpecMessage)
    places = Dict{Symbol,Peven.Place}()
    sizehint!(places, length(authored_env.places))
    for place in authored_env.places
        lowered = lower_place(place)
        places[lowered.id] = lowered
    end

    transitions = Dict{Symbol,Peven.Transition}()
    sizehint!(transitions, length(authored_env.transitions))
    arcsfrom = Peven.ArcFrom[]
    sizehint!(arcsfrom, sum(length(transition.inputs) for transition in authored_env.transitions))
    arcsto = Peven.ArcTo[]
    sizehint!(arcsto, sum(length(transition.outputs) for transition in authored_env.transitions))

    for transition in authored_env.transitions
        validate_guard_compatibility(transition)
        lowered_transition = lower_transition(transition)
        transitions[lowered_transition.id] = lowered_transition
        append!(arcsfrom, lower_input_arcs(transition))
        append!(arcsto, lower_output_arcs(transition))
    end

    return LoweredEnv(Peven.Net(places, transitions, arcsfrom, arcsto))
end

function validate_guard_compatibility(transition::TransitionSpecMessage)
    transition.guard_spec === nothing && return nothing
    if length(transition.inputs) != 1 || only(transition.inputs).weight != 1
        throw(ProtocolError(
            "the current Python guard DSL supports only single-input, weight-1 transitions",
        ))
    end
    return nothing
end

lower_place(place::PlaceSpecMessage) = Peven.Place(Symbol(place.id), place.capacity)

function lower_transition(transition::TransitionSpecMessage)
    return Peven.Transition(
        Symbol(transition.id),
        Symbol(transition.executor);
        guard=lower_guard_spec(transition.guard_spec),
        retries=transition.retries,
        join_by=lower_join_spec(transition.join_by_spec),
    )
end

function lower_input_arcs(transition::TransitionSpecMessage)
    tid = Symbol(transition.id)
    return [
        Peven.ArcFrom(tid, Symbol(arc.place), arc.weight)
        for arc in transition.inputs
    ]
end

function lower_output_arcs(transition::TransitionSpecMessage)
    tid = Symbol(transition.id)
    return [
        Peven.ArcTo(tid, Symbol(arc.place))
        for arc in transition.outputs
    ]
end

"""Validate one lowered env with the engine's public `validate(net)` API."""
validate_env(lowered_env::LoweredEnv) = Peven.validate(lowered_env.net)

"""Lower one transport marking payload into exported `Peven` token values."""
function lower_marking(initial_marking::Dict{String,Vector{TokenMessage}})
    tokens_by_place = Dict{Symbol,Vector{Peven.Token}}()
    sizehint!(tokens_by_place, length(initial_marking))
    for (place_id, bucket) in pairs(initial_marking)
        tokens_by_place[Symbol(place_id)] = [lower_token(token) for token in bucket]
    end
    return Peven.Marking(tokens_by_place)
end

"""Validate one lowered env plus initial marking with the engine's public API."""
validate_env_marking(lowered_env::LoweredEnv, marking::Peven.Marking) = Peven.validate(lowered_env.net, marking)

"""Describe validation issues as one adapter-facing error string."""
function format_validation_error(issues)
    isempty(issues) && return ""
    return join(
        ["$(issue.code):$(issue.object_id): $(issue.message)" for issue in issues],
        "; ",
    )
end

"""Lower one guard spec into the engine's `guard(tokens) -> Bool` callback shape."""
function lower_guard_spec(spec)
    spec === nothing && return nothing
    return function (tokens)
        isempty(tokens) && throw(ProtocolError("guard requires at least one input token"))
        payload = getfield(tokens[1], :payload)
        value = _eval_guard_node(spec, payload)
        value isa Bool || throw(ProtocolError("guard root must evaluate to Bool"))
        return value
    end
end

"""Lower one join selector spec into the engine's `(place_id, token) -> key` shape."""
function lower_join_spec(spec)
    spec === nothing && return nothing
    return function (place_id, token)
        return _eval_join_node(spec, place_id, getfield(token, :payload))
    end
end

lower_token(token::TokenMessage) = Peven.Token(Symbol(token.color), token.run_key, token.payload)

function _eval_guard_node(node, payload)
    kind = _require_kind(node)
    if kind == "field_ref"
        return _lookup_payload_path(payload, _require_string_vector(node, "path"))
    elseif kind == "literal"
        return get(node, "value", nothing)
    elseif kind == "cmp"
        left = _eval_guard_node(_require_dict_field(node, "left"), payload)
        right = _eval_guard_node(_require_dict_field(node, "right"), payload)
        op = _require_nonempty_string(node, "op")
        return _compare_guard_values(op, left, right)
    elseif kind == "call"
        name = _require_nonempty_string(node, "name")
        args = [_eval_guard_node(arg, payload) for arg in _require_vector(node, "args")]
        return _eval_guard_call(name, args)
    elseif kind == "in"
        ref = _eval_guard_node(_require_dict_field(node, "ref"), payload)
        values = [
            _eval_guard_node(value, payload)
            for value in _require_vector(node, "values")
        ]
        return ref in values
    elseif kind == "and"
        for child in _require_vector(node, "children")
            value = _eval_guard_node(child, payload)
            value isa Bool || throw(ProtocolError("guard and children must evaluate to Bool"))
            value || return false
        end
        return true
    elseif kind == "or"
        for child in _require_vector(node, "children")
            value = _eval_guard_node(child, payload)
            value isa Bool || throw(ProtocolError("guard or children must evaluate to Bool"))
            value && return true
        end
        return false
    elseif kind == "not"
        value = _eval_guard_node(_require_dict_field(node, "child"), payload)
        value isa Bool || throw(ProtocolError("guard not child must evaluate to Bool"))
        return !value
    end
    throw(ProtocolError("unsupported guard node kind $(repr(kind))"))
end

function _compare_guard_values(op::String, left, right)
    if op == "=="
        return left == right
    elseif op == "!="
        return left != right
    elseif op == "<"
        return left < right
    elseif op == "<="
        return left <= right
    elseif op == ">"
        return left > right
    elseif op == ">="
        return left >= right
    end
    throw(ProtocolError("unsupported guard comparison op $(repr(op))"))
end

function _eval_guard_call(name::String, args::Vector)
    if name == "isnothing"
        length(args) == 1 || throw(ProtocolError("guard call isnothing expects 1 argument"))
        return isnothing(args[1])
    elseif name == "isempty"
        length(args) == 1 || throw(ProtocolError("guard call isempty expects 1 argument"))
        return isempty(args[1])
    elseif name == "length"
        length(args) == 1 || throw(ProtocolError("guard call length expects 1 argument"))
        return length(args[1])
    end
    throw(ProtocolError("unsupported guard call $(repr(name))"))
end

function _eval_join_node(node, place_id, payload)
    kind = _require_kind(node)
    if kind == "payload_ref"
        return _lookup_payload_path(payload, _require_string_vector(node, "path"))
    elseif kind == "place_id"
        return String(place_id)
    elseif kind == "literal"
        return get(node, "value", nothing)
    elseif kind == "tuple"
        return Tuple(
            [
            _eval_join_node(item, place_id, payload)
            for item in _require_vector(node, "items")
            ],
        )
    end
    throw(ProtocolError("unsupported join node kind $(repr(kind))"))
end

function _lookup_payload_path(payload, path::Vector{String})
    current = payload
    for segment in path
        current = _lookup_payload_segment(current, segment)
    end
    return current
end

function _lookup_payload_segment(payload::AbstractDict, segment::String)
    if haskey(payload, segment)
        return payload[segment]
    elseif haskey(payload, Symbol(segment))
        return payload[Symbol(segment)]
    end
    throw(ProtocolError("payload path segment $(repr(segment)) was not found"))
end

function _lookup_payload_segment(payload, segment::String)
    throw(ProtocolError("cannot read payload path segment $(repr(segment)) from $(typeof(payload))"))
end

end # module Lowering
