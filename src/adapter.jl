module Adapter

using Peven

using ..AuthoredIR:
    decode_initial_marking,
    decode_load_env_message,
    decode_run_env_message,
    _require_key,
    _require_nonempty_string,
    _require_positive_integer
using ..Lowering:
    LoweredEnv,
    format_validation_error,
    lower_env,
    lower_marking,
    lower_token,
    validate_env,
    validate_env_marking
using ..Protocol: ProtocolError

mutable struct AdapterState
    loaded_env::Union{Nothing, LoweredEnv}
    next_adapter_req_id::Int
    req_id_lock::ReentrantLock
end

AdapterState() = AdapterState(nothing, 2, ReentrantLock())

"""Transport hook for emitting one runtime message to the Python peer."""
emit_message!(transport, message) = throw(ProtocolError("transport must implement emit_message!"))

"""Transport hook for one callback request/reply exchange with the Python peer."""
exchange_callback!(transport, request) = throw(ProtocolError("transport must implement exchange_callback!"))

"""Mark the transport poisoned after a protocol or IO failure."""
poison_transport!(transport) = nothing

"""Raised when Python rejects one callback execution."""
struct CallbackExecutionError <: Exception
    message::String
end

Base.showerror(io::IO, error::CallbackExecutionError) = print(io, error.message)

struct PreparedRun
    env_run_id::Int
    marking::Peven.Marking
    fuse::Union{Nothing,Int}
end

struct PythonCallbackExecutor{TTransport} <: Peven.AbstractExecutor
    state::AdapterState
    transport::TTransport
    env_run_id::Int
end

"""Handle one unpacked adapter message and return the reply plus any accepted run."""
function accept_message!(state::AdapterState, message)
    message isa AbstractDict || throw(ProtocolError("adapter message must be a map"))
    kind = get(message, "kind", nothing)
    kind isa AbstractString || throw(ProtocolError("adapter message kind must be a string"))
    if kind == "load_env"
        return (; reply=_handle_load_env!(state, message), accepted_run=nothing)
    elseif kind == "run_env"
        return _handle_run_env!(state, message)
    end
    throw(ProtocolError("unsupported adapter message kind $(repr(kind))"))
end

"""Execute one already-accepted run and stream callbacks/events through the transport."""
function execute_loaded_run!(state::AdapterState, prepared_run::PreparedRun, transport)
    loaded_env = state.loaded_env
    loaded_env === nothing && throw(ProtocolError("run_env requires a previously loaded env"))
    executors = _python_executors(state, loaded_env, prepared_run.env_run_id, transport)
    on_event = event -> emit_message!(transport, _runtime_event_message(prepared_run.env_run_id, event))
    if isnothing(prepared_run.fuse)
        return Peven.fire(
            loaded_env.net,
            prepared_run.marking;
            executors=executors,
            on_event=on_event,
            on_event_error=:throw,
        )
    end
    return Peven.fire(
        loaded_env.net,
        prepared_run.marking;
        fuse=prepared_run.fuse,
        executors=executors,
        on_event=on_event,
        on_event_error=:throw,
    )
end

function Peven.execute(executor::PythonCallbackExecutor, ctx::Peven.ExecutionContext)
    req_id = _allocate_adapter_req_id!(executor.state)
    request = _callback_request_message(req_id, executor.env_run_id, ctx)
    reply = try
        exchange_callback!(executor.transport, request)
    catch error
        error isa InterruptException && rethrow()
        poison_transport!(executor.transport)
        error isa ProtocolError && rethrow()
        throw(ProtocolError("callback exchange failed: $(sprint(showerror, error))"))
    end
    try
        return _decode_callback_reply(reply; req_id=req_id, env_run_id=executor.env_run_id)
    catch error
        error isa InterruptException && rethrow()
        error isa CallbackExecutionError && rethrow()
        poison_transport!(executor.transport)
        error isa ProtocolError && rethrow()
        throw(ProtocolError("callback reply decode failed: $(sprint(showerror, error))"))
    end
end

function _handle_load_env!(state::AdapterState, message::AbstractDict)
    req_id = _require_positive_integer(message, "req_id")
    load_env = try
        decode_load_env_message(message)
    catch error
        error isa ProtocolError || rethrow()
        return Dict(
            "kind" => "load_env_error",
            "req_id" => req_id,
            "error" => error.message,
        )
    end
    lowered_env = try
        lower_env(load_env.env)
    catch error
        error isa ProtocolError || rethrow()
        return Dict(
            "kind" => "load_env_error",
            "req_id" => req_id,
            "error" => error.message,
        )
    end
    issues = validate_env(lowered_env)
    if !isempty(issues)
        return Dict(
            "kind" => "load_env_error",
            "req_id" => req_id,
            "error" => format_validation_error(issues),
        )
    end
    state.loaded_env = lowered_env
    return Dict(
        "kind" => "load_env_ok",
        "req_id" => req_id,
    )
end

function _handle_run_env!(state::AdapterState, message::AbstractDict)
    req_id = _require_positive_integer(message, "req_id")
    env_run_id = _require_positive_integer(message, "env_run_id")
    loaded_env = state.loaded_env
    if loaded_env === nothing
        return (
            reply=Dict(
                "kind" => "run_env_error",
                "req_id" => req_id,
                "env_run_id" => env_run_id,
                "error" => "run_env requires a previously loaded env",
            ),
            accepted_run=nothing,
        )
    end
    run_env = try
        decode_run_env_message(message)
    catch error
        error isa ProtocolError || rethrow()
        return (
            reply=Dict(
                "kind" => "run_env_error",
                "req_id" => req_id,
                "env_run_id" => env_run_id,
                "error" => error.message,
            ),
            accepted_run=nothing,
        )
    end
    marking = lower_marking(run_env.initial_marking)
    issues = validate_env_marking(loaded_env, marking)
    if !isempty(issues)
        return (
            reply=Dict(
                "kind" => "run_env_error",
                "req_id" => req_id,
                "env_run_id" => env_run_id,
                "error" => format_validation_error(issues),
            ),
            accepted_run=nothing,
        )
    end
    return (
        reply=Dict(
            "kind" => "run_env_ok",
            "req_id" => req_id,
            "env_run_id" => env_run_id,
        ),
        accepted_run=PreparedRun(env_run_id, marking, run_env.fuse),
    )
end

function _python_executors(
    state::AdapterState,
    loaded_env::LoweredEnv,
    env_run_id::Int,
    transport,
)
    executors = Dict{Symbol,Peven.AbstractExecutor}()
    for transition in values(loaded_env.net.transitions)
        executors[transition.executor] = PythonCallbackExecutor(state, transport, env_run_id)
    end
    return executors
end

function _allocate_adapter_req_id!(state::AdapterState)
    lock(state.req_id_lock)
    try
        req_id = state.next_adapter_req_id
        state.next_adapter_req_id += 2
        return req_id
    finally
        unlock(state.req_id_lock)
    end
end

function _callback_request_message(req_id::Int, env_run_id::Int, ctx::Peven.ExecutionContext)
    return Dict(
        "kind" => "callback_request",
        "req_id" => req_id,
        "env_run_id" => env_run_id,
        "transition_id" => String(ctx.transition_id),
        "bundle" => _bundle_message(ctx.bundle),
        "tokens" => [_token_message(token) for token in ctx.tokens],
        "inputs_by_place" => _token_buckets_message(ctx.inputs_by_place),
        "attempt" => ctx.attempt,
    )
end

function _decode_callback_reply(reply; req_id::Int, env_run_id::Int)
    reply isa AbstractDict || throw(ProtocolError("callback reply must be a map"))
    kind = get(reply, "kind", nothing)
    kind isa AbstractString || throw(ProtocolError("callback reply kind must be a string"))
    _require_matching_positive_integer(reply, "req_id", req_id)
    _require_matching_positive_integer(reply, "env_run_id", env_run_id)
    if kind == "callback_reply"
        outputs = decode_initial_marking(
            _require_key(reply, "outputs");
            context="callback reply outputs",
        )
        return Dict(
            Symbol(place_id) => [lower_token(token) for token in bucket]
            for (place_id, bucket) in pairs(outputs)
        )
    elseif kind == "callback_error"
        error_message = _require_nonempty_string(reply, "error")
        throw(CallbackExecutionError(error_message))
    end
    throw(ProtocolError("unsupported callback reply kind $(repr(kind))"))
end

function _runtime_event_message(env_run_id::Int, event::Peven.TransitionStarted)
    return Dict(
        "kind" => "transition_started",
        "env_run_id" => env_run_id,
        "bundle" => _bundle_message(event.bundle),
        "firing_id" => event.firing_id,
        "attempt" => event.attempt,
        "inputs" => [_token_message(token) for token in event.inputs],
        "inputs_by_place" => _token_buckets_message(event.inputs_by_place),
    )
end

function _runtime_event_message(env_run_id::Int, event::Peven.TransitionCompleted)
    return Dict(
        "kind" => "transition_completed",
        "env_run_id" => env_run_id,
        "bundle" => _bundle_message(event.bundle),
        "firing_id" => event.firing_id,
        "attempt" => event.attempt,
        "outputs" => _token_buckets_message(event.outputs),
    )
end

function _runtime_event_message(env_run_id::Int, event::Peven.TransitionFailed)
    return Dict(
        "kind" => "transition_failed",
        "env_run_id" => env_run_id,
        "bundle" => _bundle_message(event.bundle),
        "firing_id" => event.firing_id,
        "attempt" => event.attempt,
        "error" => event.error,
        "retrying" => event.retrying,
    )
end

function _runtime_event_message(env_run_id::Int, event::Peven.GuardErrored)
    return Dict(
        "kind" => "guard_errored",
        "env_run_id" => env_run_id,
        "bundle" => _bundle_message(event.bundle),
        "error" => event.error,
    )
end

function _runtime_event_message(env_run_id::Int, event::Peven.SelectionErrored)
    return Dict(
        "kind" => "selection_errored",
        "env_run_id" => env_run_id,
        "transition_id" => String(event.transition_id),
        "run_key" => event.run_key,
        "error" => event.error,
    )
end

function _runtime_event_message(env_run_id::Int, event::Peven.RunFinished)
    return Dict(
        "kind" => "run_finished",
        "env_run_id" => env_run_id,
        "result" => _run_result_message(event.result),
    )
end

function _run_result_message(result::Peven.RunResult)
    return Dict(
        "run_key" => result.run_key,
        "status" => String(result.status),
        "error" => result.error,
        "terminal_reason" => _string_or_nothing(result.terminal_reason),
        "terminal_bundle" => isnothing(result.terminal_bundle) ? nothing : _bundle_message(result.terminal_bundle),
        "terminal_transition" => _string_or_nothing(result.terminal_transition),
        "trace" => [_transition_result_message(item) for item in result.trace],
        "final_marking" => _marking_message(result.final_marking),
    )
end

function _transition_result_message(result::Peven.TransitionResult)
    return Dict(
        "bundle" => _bundle_message(result.bundle),
        "firing_id" => result.firing_id,
        "status" => String(result.status),
        "outputs" => _token_buckets_message(result.outputs),
        "error" => result.error,
        "attempts" => result.attempts,
    )
end

function _marking_message(marking::Peven.Marking)
    return Dict(
        String(place_id) => [_token_message(token) for token in bucket]
        for (place_id, bucket) in pairs(marking.tokens_by_place)
    )
end

function _token_buckets_message(outputs::AbstractDict)
    return Dict(
        String(place_id) => [_token_message(token) for token in bucket]
        for place_id in sort!(collect(keys(outputs)))
        for bucket = (outputs[place_id],)
    )
end

function _bundle_message(bundle::Peven.BundleRef)
    return Dict(
        "transition_id" => String(bundle.transition_id),
        "run_key" => bundle.run_key,
        "selected_key" => bundle.selected_key,
        "ordinal" => bundle.ordinal,
    )
end

function _token_message(token::Peven.Token)
    return Dict(
        "run_key" => Peven.run_key(token),
        "color" => String(Peven.color(token)),
        "payload" => getfield(token, :payload),
    )
end

function _token_message(token::Peven.AbstractToken)
    throw(ProtocolError("adapter can only serialize Peven.Token values"))
end

_string_or_nothing(value::Nothing) = nothing
_string_or_nothing(value::Symbol) = String(value)

function _require_matching_positive_integer(message::AbstractDict, key::String, expected::Int)
    value = _require_positive_integer(message, key)
    value == expected || throw(ProtocolError("$(key) did not match the request"))
    return value
end

end # module Adapter
