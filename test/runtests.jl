using Test

using Peven
using PevenPy

place_spec(id; capacity=nothing, schema=nothing) = Dict(
    "id" => id,
    "capacity" => capacity,
    "schema" => schema,
)

function input_arc(place; weight=1, optional=false)
    arc = Dict("place" => place, "weight" => weight)
    optional && (arc["optional"] = optional)
    return arc
end
output_arc(place) = Dict("place" => place)

transition_spec(
    id;
    executor="finish_executor",
    inputs=Any[],
    outputs=Any[],
    guard_spec=nothing,
    retries=0,
    join_by_spec=nothing,
) = Dict(
    "id" => id,
    "executor" => executor,
    "inputs" => inputs,
    "outputs" => outputs,
    "guard_spec" => guard_spec,
    "retries" => retries,
    "join_by_spec" => join_by_spec,
)

function env_spec_message(
    ;
    env_name="phase_2_env",
    places=Any[
        place_spec("ready"),
        place_spec("done"),
    ],
    transitions=Any[
        transition_spec(
            "finish";
            inputs=Any[input_arc("ready")],
            outputs=Any[output_arc("done")],
        ),
    ],
)
    return Dict(
        "schema_version" => 1,
        "env_name" => env_name,
        "places" => places,
        "transitions" => transitions,
    )
end

load_env_request(env; req_id=1) = Dict(
    "kind" => "load_env",
    "req_id" => req_id,
    "env" => env,
)

token_message(; run_key="rk-1", color="default", payload=nothing) = Dict(
    "run_key" => run_key,
    "color" => color,
    "payload" => payload,
)

function run_env_request(;
    req_id=1,
    env_run_id=1,
    initial_marking=Dict("ready" => Any[token_message()]),
    fuse=nothing,
)
    request = Dict(
        "kind" => "run_env",
        "req_id" => req_id,
        "env_run_id" => env_run_id,
        "initial_marking" => initial_marking,
    )
    if !isnothing(fuse)
        request["fuse"] = fuse
    end
    return request
end

adapter_reply(state, message) = PevenPy.Adapter.accept_message!(state, message).reply

function load_env_reply(env; req_id=1)
    state = PevenPy.Adapter.AdapterState()
    return adapter_reply(state, load_env_request(env; req_id=req_id))
end

mutable struct FakeTransport
    messages::Vector{Any}
    callback_replies::Vector{Any}
    poisoned::Bool
    fail_event_kind::Union{Nothing,String}
    lock::ReentrantLock
end

FakeTransport(; callback_replies=Any[], fail_event_kind=nothing) = FakeTransport(
    Any[],
    copy(callback_replies),
    false,
    fail_event_kind,
    ReentrantLock(),
)

function PevenPy.Adapter.emit_message!(transport::FakeTransport, message)
    lock(transport.lock)
    try
        transport.poisoned && throw(PevenPy.Protocol.ProtocolError("transport is poisoned"))
        if !isnothing(transport.fail_event_kind) &&
           get(message, "kind", nothing) == transport.fail_event_kind
            throw(PevenPy.Protocol.ProtocolError(
                "injected runtime event failure for kind $(repr(transport.fail_event_kind))",
            ))
        end
        push!(transport.messages, message)
    finally
        unlock(transport.lock)
    end
    return nothing
end

function PevenPy.Adapter.exchange_callback!(transport::FakeTransport, request)
    lock(transport.lock)
    try
        push!(transport.messages, request)
        isempty(transport.callback_replies) && error("missing callback reply")
        return popfirst!(transport.callback_replies)
    finally
        unlock(transport.lock)
    end
end

function PevenPy.Adapter.poison_transport!(transport::FakeTransport)
    transport.poisoned = true
    return nothing
end

@testset "protocol" begin
    payload = PevenPy.Protocol.encode_message(PevenPy.Protocol.handshake_message())
    @test !isempty(payload)
    @test PevenPy.Protocol.MAX_FRAME_BYTES == 8 * 1024 * 1024

    frame = PevenPy.Protocol.handshake_frame()
    @test length(frame) == 4 + length(payload)
    @test frame[1:4] == UInt8[0x00, 0x00, 0x00, UInt8(length(payload))]
    @test frame[5:end] == payload

    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Protocol.encode_frame(
        fill(UInt8(0), PevenPy.Protocol.MAX_FRAME_BYTES + 1),
    )

    oversized_prefix = reinterpret(UInt8, [hton(UInt32(PevenPy.Protocol.MAX_FRAME_BYTES + 1))])
    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Protocol.read_frame(
        IOBuffer(oversized_prefix),
    )

    framed_payload = PevenPy.Protocol.encode_frame(UInt8[0x01, 0x02, 0x03])
    @test PevenPy.Protocol.read_frame(IOBuffer(framed_payload)) == UInt8[0x01, 0x02, 0x03]
    @test isnothing(PevenPy.Protocol.read_frame(IOBuffer(UInt8[])))

    struct ExplodingIO <: IO end
    Base.read(::ExplodingIO, ::Type{UInt32}) = throw(ArgumentError("boom"))
    @test_throws ArgumentError PevenPy.Protocol.read_frame(ExplodingIO())
end

@testset "authored ir decode" begin
    request = load_env_request(env_spec_message())

    decoded = PevenPy.AuthoredIR.decode_load_env_message(request)
    @test decoded.req_id == 1
    @test decoded.env.env_name == "phase_2_env"
    @test length(decoded.env.places) == 2
    @test length(decoded.env.transitions) == 1
    @test only(decoded.env.transitions).inputs[1].optional === false

    optional_arc = PevenPy.AuthoredIR.decode_input_arc_spec_message(input_arc("plan"; optional=true))
    @test optional_arc.place == "plan"
    @test optional_arc.weight == 1
    @test optional_arc.optional === true
    @test PevenPy.AuthoredIR.decode_input_arc_spec_message(Dict("place" => "plan")).optional === false
    @test_throws PevenPy.Protocol.ProtocolError PevenPy.AuthoredIR.decode_input_arc_spec_message(
        Dict("place" => "plan", "optional" => "yes"),
    )

    run_decoded = PevenPy.AuthoredIR.decode_run_env_message(run_env_request())
    @test run_decoded.req_id == 1
    @test run_decoded.env_run_id == 1
    @test isnothing(run_decoded.fuse)
    @test only(keys(run_decoded.initial_marking)) == "ready"
    @test first(run_decoded.initial_marking["ready"]).run_key == "rk-1"

    fused = PevenPy.AuthoredIR.decode_run_env_message(run_env_request(fuse=7))
    @test fused.fuse == 7

    @test_throws PevenPy.Protocol.ProtocolError PevenPy.AuthoredIR.decode_load_env_message(
        Dict("kind" => "load_env", "req_id" => 1, "env" => Dict("env_name" => "x"))
    )
    @test_throws PevenPy.Protocol.ProtocolError PevenPy.AuthoredIR.decode_run_env_message(
        Dict("kind" => "run_env", "req_id" => 1, "env_run_id" => 1, "initial_marking" => [])
    )
end

@testset "lowering" begin
    lowered = PevenPy.Lowering.lower_env(
        PevenPy.AuthoredIR.decode_env_spec_message(env_spec_message())
    )
    @test lowered.net isa Peven.Net
    @test isempty(PevenPy.Lowering.validate_env(lowered))
    @test only(lowered.net.arcsfrom).optional === false

    optional_env = env_spec_message(
        env_name="optional_env",
        places=Any[place_spec("ready"), place_spec("plan"), place_spec("done")],
        transitions=Any[
            transition_spec(
                "finish";
                inputs=Any[input_arc("ready"), input_arc("plan"; optional=true)],
                outputs=Any[output_arc("done")],
            ),
        ],
    )
    optional_lowered = PevenPy.Lowering.lower_env(
        PevenPy.AuthoredIR.decode_env_spec_message(optional_env),
    )
    @test isempty(PevenPy.Lowering.validate_env(optional_lowered))
    required_arc = only([arc for arc in optional_lowered.net.arcsfrom if arc.from === :ready])
    optional_plan_arc = only([arc for arc in optional_lowered.net.arcsfrom if arc.from === :plan])
    @test required_arc.optional === false
    @test optional_plan_arc.optional === true

    guard_spec = Dict(
        "kind" => "and",
        "children" => Any[
            Dict(
                "kind" => "cmp",
                "op" => "==",
                "left" => Dict("kind" => "field_ref", "path" => Any["status"]),
                "right" => Dict("kind" => "literal", "value" => "ready"),
            ),
            Dict(
                "kind" => "cmp",
                "op" => ">=",
                "left" => Dict(
                    "kind" => "call",
                    "name" => "length",
                    "args" => Any[
                        Dict("kind" => "field_ref", "path" => Any["items"]),
                    ],
                ),
                "right" => Dict("kind" => "literal", "value" => 2),
            ),
        ],
    )
    guard = PevenPy.Lowering.lower_guard_spec(guard_spec)
    @test guard([Peven.Token(:default, "rk-1", Dict("status" => "ready", "items" => Any[1, 2]))]) === true
    @test guard([Peven.Token(:default, "rk-1", Dict("status" => "hold", "items" => Any[1]))]) === false

    join_spec = Dict(
        "kind" => "tuple",
        "items" => Any[
            Dict("kind" => "place_id"),
            Dict("kind" => "payload_ref", "path" => Any["case_id"]),
            Dict("kind" => "literal", "value" => "judge"),
        ],
    )
    join_by = PevenPy.Lowering.lower_join_spec(join_spec)
    @test join_by(:left, Peven.Token(:default, "rk-1", Dict("case_id" => 7))) == ("left", 7, "judge")

    malformed_guard = PevenPy.Lowering.lower_guard_spec(
        Dict("kind" => "field_ref", "path" => Any[1]),
    )
    @test_throws PevenPy.Protocol.ProtocolError malformed_guard(
        [Peven.Token(:default, "rk-1", Dict("status" => "ready"))],
    )

    malformed_join = PevenPy.Lowering.lower_join_spec(
        Dict("kind" => "payload_ref", "path" => Any[1]),
    )
    @test_throws PevenPy.Protocol.ProtocolError malformed_join(
        :left,
        Peven.Token(:default, "rk-1", Dict("case_id" => 7)),
    )

    payload = Dict(
        "status" => "ready",
        "items" => Any[1, 2],
        "empty_items" => Any[],
        "count" => 2,
        "missing" => nothing,
    )
    guard_token = Peven.Token(:default, "rk-1", payload)

    in_guard = PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "in",
            "ref" => Dict("kind" => "field_ref", "path" => Any["status"]),
            "values" => Any[
                Dict("kind" => "literal", "value" => "hold"),
                Dict("kind" => "literal", "value" => "ready"),
            ],
        ),
    )
    @test in_guard([guard_token]) === true

    or_guard = PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "or",
            "children" => Any[
                Dict(
                    "kind" => "cmp",
                    "op" => "==",
                    "left" => Dict("kind" => "field_ref", "path" => Any["status"]),
                    "right" => Dict("kind" => "literal", "value" => "hold"),
                ),
                Dict(
                    "kind" => "cmp",
                    "op" => "==",
                    "left" => Dict("kind" => "field_ref", "path" => Any["status"]),
                    "right" => Dict("kind" => "literal", "value" => "ready"),
                ),
            ],
        ),
    )
    @test or_guard([guard_token]) === true

    not_guard = PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "not",
            "child" => Dict(
                "kind" => "cmp",
                "op" => "==",
                "left" => Dict("kind" => "field_ref", "path" => Any["status"]),
                "right" => Dict("kind" => "literal", "value" => "hold"),
            ),
        ),
    )
    @test not_guard([guard_token]) === true

    @test PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "cmp",
            "op" => "!=",
            "left" => Dict("kind" => "field_ref", "path" => Any["status"]),
            "right" => Dict("kind" => "literal", "value" => "hold"),
        ),
    )([guard_token]) === true
    @test PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "cmp",
            "op" => "<",
            "left" => Dict(
                "kind" => "call",
                "name" => "length",
                "args" => Any[Dict("kind" => "field_ref", "path" => Any["items"])],
            ),
            "right" => Dict("kind" => "literal", "value" => 3),
        ),
    )([guard_token]) === true
    @test PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "cmp",
            "op" => "<=",
            "left" => Dict("kind" => "field_ref", "path" => Any["count"]),
            "right" => Dict("kind" => "literal", "value" => 2),
        ),
    )([guard_token]) === true
    @test PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "cmp",
            "op" => ">",
            "left" => Dict("kind" => "field_ref", "path" => Any["count"]),
            "right" => Dict("kind" => "literal", "value" => 1),
        ),
    )([guard_token]) === true

    @test PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "call",
            "name" => "isnothing",
            "args" => Any[Dict("kind" => "field_ref", "path" => Any["missing"])],
        ),
    )([guard_token]) === true
    @test PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "call",
            "name" => "isempty",
            "args" => Any[Dict("kind" => "field_ref", "path" => Any["empty_items"])],
        ),
    )([guard_token]) === true

    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Lowering.lower_guard_spec(
        Dict(
            "kind" => "cmp",
            "op" => "~=",
            "left" => Dict("kind" => "field_ref", "path" => Any["status"]),
            "right" => Dict("kind" => "literal", "value" => "ready"),
        ),
    )([guard_token])
    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Lowering.lower_guard_spec(
        Dict("kind" => "call", "name" => "unknown", "args" => Any[]),
    )([guard_token])
    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Lowering.lower_guard_spec(
        Dict("kind" => "mystery"),
    )([guard_token])
    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Lowering.lower_join_spec(
        Dict("kind" => "mystery"),
    )(:left, guard_token)

    guarded_env = env_spec_message(
        env_name="guarded_env",
        transitions=Any[
            transition_spec(
                "finish";
                inputs=Any[input_arc("ready")],
                outputs=Any[output_arc("done")],
                guard_spec=guard_spec,
            ),
        ],
    )
    @test isempty(
        PevenPy.Lowering.validate_env(
            PevenPy.Lowering.lower_env(
                PevenPy.AuthoredIR.decode_env_spec_message(guarded_env),
            ),
        ),
    )

    keyed_join_env = env_spec_message(
        env_name="keyed_join_env",
        places=Any[place_spec("left"), place_spec("right"), place_spec("done")],
        transitions=Any[
            transition_spec(
                "join";
                inputs=Any[input_arc("left"), input_arc("right")],
                outputs=Any[output_arc("done")],
                join_by_spec=join_spec,
            ),
        ],
    )
    @test isempty(
        PevenPy.Lowering.validate_env(
            PevenPy.Lowering.lower_env(
                PevenPy.AuthoredIR.decode_env_spec_message(keyed_join_env),
            ),
        ),
    )

    zero_output_env = env_spec_message(
        env_name="zero_output_env",
        places=Any[place_spec("ready")],
        transitions=Any[
            transition_spec(
                "sink";
                inputs=Any[input_arc("ready")],
                outputs=Any[],
            ),
        ],
    )
    @test isempty(
        PevenPy.Lowering.validate_env(
            PevenPy.Lowering.lower_env(
                PevenPy.AuthoredIR.decode_env_spec_message(zero_output_env),
            ),
        ),
    )

    marking = PevenPy.Lowering.lower_marking(
        Dict("ready" => [PevenPy.AuthoredIR.TokenMessage("rk-1", "default", Dict("seed" => 7))]),
    )
    @test marking isa Peven.Marking
    @test isempty(PevenPy.Lowering.validate_env_marking(lowered, marking))
end

@testset "adapter load_env and run_env" begin
    state = PevenPy.Adapter.AdapterState()
    reply = adapter_reply(state, load_env_request(env_spec_message()))
    @test reply == Dict("kind" => "load_env_ok", "req_id" => 1)
    @test state.loaded_env !== nothing
    @test isempty(Peven.validate(state.loaded_env.net))

    invalid_reply = adapter_reply(
        state,
        Dict("kind" => "load_env", "req_id" => 5, "env" => Dict("env_name" => "bad")),
    )
    @test invalid_reply == Dict(
        "kind" => "load_env_error",
        "req_id" => 5,
        "error" => "missing required field \"schema_version\"",
    )

    run_reply = adapter_reply(
        state,
        run_env_request(),
    )
    @test run_reply == Dict(
        "kind" => "run_env_ok",
        "req_id" => 1,
        "env_run_id" => 1,
    )

    no_env_reply = adapter_reply(
        PevenPy.Adapter.AdapterState(),
        run_env_request(req_id=4, env_run_id=9),
    )
    @test no_env_reply == Dict(
        "kind" => "run_env_error",
        "req_id" => 4,
        "env_run_id" => 9,
        "error" => "run_env requires a previously loaded env",
    )

    malformed_marking_reply = adapter_reply(
        state,
        Dict(
            "kind" => "run_env",
            "req_id" => 5,
            "env_run_id" => 2,
            "initial_marking" => Dict("ready" => 1),
        ),
    )
    @test malformed_marking_reply == Dict(
        "kind" => "run_env_error",
        "req_id" => 5,
        "env_run_id" => 2,
        "error" => "marking buckets must be lists",
    )

    accepted = PevenPy.Adapter.accept_message!(state, run_env_request(req_id=9, env_run_id=4))
    @test accepted.reply == Dict(
        "kind" => "run_env_ok",
        "req_id" => 9,
        "env_run_id" => 4,
    )
    @test accepted.accepted_run isa PevenPy.Adapter.PreparedRun
    @test accepted.accepted_run.env_run_id == 4
    @test isnothing(accepted.accepted_run.fuse)
    @test haskey(accepted.accepted_run.marking.tokens_by_place, :ready)

    fused_accepted = PevenPy.Adapter.accept_message!(
        state,
        run_env_request(req_id=11, env_run_id=5, fuse=9),
    )
    @test fused_accepted.accepted_run.fuse == 9

    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Adapter.accept_message!(
        state,
        Dict("kind" => "mystery"),
    )
end

@testset "adapter load_env engine validation coverage via authored IR" begin
    @test load_env_reply(
        env_spec_message(
            places=Any[place_spec("ready"), place_spec("done")],
            transitions=Any[
                transition_spec(
                    "finish";
                    inputs=Any[input_arc("missing")],
                    outputs=Any[output_arc("done")],
                ),
            ],
        ),
    )["error"] |> x -> occursin("unknown_place:missing", x)

    @test load_env_reply(
        env_spec_message(
            places=Any[place_spec("ready"), place_spec("done")],
            transitions=Any[
                transition_spec(
                    "finish";
                    inputs=Any[input_arc("ready")],
                    outputs=Any[output_arc("missing")],
                ),
            ],
        ),
    )["error"] |> x -> occursin("unknown_place:missing", x)

    @test load_env_reply(
        env_spec_message(
            places=Any[place_spec("left"), place_spec("done")],
            transitions=Any[
                transition_spec(
                    "join";
                    inputs=Any[input_arc("left"), input_arc("left", weight=2)],
                    outputs=Any[output_arc("done")],
                ),
            ],
        ),
    )["error"] |> x -> occursin("duplicate_input_arc:join", x)

    @test load_env_reply(
        env_spec_message(
            places=Any[place_spec("ready"), place_spec("done")],
            transitions=Any[
                transition_spec(
                    "finish";
                    inputs=Any[input_arc("ready")],
                    outputs=Any[output_arc("done"), output_arc("done")],
                ),
            ],
        ),
    )["error"] |> x -> occursin("duplicate_output_arc:finish", x)

    @test load_env_reply(
        env_spec_message(
            places=Any[place_spec("left"), place_spec("done")],
            transitions=Any[
                transition_spec(
                    "join";
                    inputs=Any[input_arc("left")],
                    outputs=Any[output_arc("done")],
                    join_by_spec=Dict("kind" => "payload_ref", "path" => Any["case_id"]),
                ),
            ],
        ),
    )["error"] |> x -> occursin("invalid_keyed_join:join", x)

    @test load_env_reply(
        env_spec_message(
            places=Any[place_spec("ready", capacity=1), place_spec("done")],
            transitions=Any[
                transition_spec(
                    "finish";
                    inputs=Any[input_arc("ready", weight=2)],
                    outputs=Any[output_arc("done")],
                ),
            ],
        ),
    )["error"] |> x -> occursin("weight_exceeds_capacity:finish", x)

    @test load_env_reply(
        env_spec_message(
            places=Any[place_spec("ready"), place_spec("done"), place_spec("lonely")],
            transitions=Any[
                transition_spec(
                    "finish";
                    inputs=Any[input_arc("ready")],
                    outputs=Any[output_arc("done")],
                ),
            ],
        ),
    )["error"] |> x -> occursin("orphan_place:lonely", x)

    @test load_env_reply(
        env_spec_message(
            places=Any[place_spec("ready"), place_spec("done")],
            transitions=Any[
                transition_spec(
                    "weighted_guard";
                    inputs=Any[input_arc("ready", weight=2)],
                    outputs=Any[output_arc("done")],
                    guard_spec=Dict(
                        "kind" => "not",
                        "child" => Dict(
                            "kind" => "call",
                            "name" => "isempty",
                            "args" => Any[Dict("kind" => "field_ref", "path" => Any["items"])],
                        ),
                    ),
                ),
            ],
        ),
    )["error"] |> x -> occursin("single-input, weight-1 transitions", x)
end

@testset "adapter run_env engine validation coverage via initial marking" begin
    state = PevenPy.Adapter.AdapterState()
    constrained_env = env_spec_message(
        places=Any[place_spec("ready", capacity=1), place_spec("done")],
    )
    @test adapter_reply(state, load_env_request(constrained_env)) == Dict(
        "kind" => "load_env_ok",
        "req_id" => 1,
    )

    @test adapter_reply(
        state,
        run_env_request(
            req_id=2,
            env_run_id=3,
            initial_marking=Dict("missing" => Any[token_message()]),
        ),
    )["error"] |> x -> occursin("unknown_place:missing", x)

    @test adapter_reply(
        state,
        run_env_request(
            req_id=3,
            env_run_id=4,
            initial_marking=Dict("ready" => Any[token_message(), token_message(run_key="rk-2")]),
        ),
    )["error"] |> x -> occursin("capacity_exceeded:ready", x)

    unreachable_state = PevenPy.Adapter.AdapterState()
    unreachable_env = env_spec_message(
        env_name="unreachable_env",
        places=Any[place_spec("ready"), place_spec("done"), place_spec("other"), place_spec("extra")],
        transitions=Any[
            transition_spec(
                "finish";
                inputs=Any[input_arc("ready")],
                outputs=Any[output_arc("done")],
            ),
            transition_spec(
                "score";
                inputs=Any[input_arc("other")],
                outputs=Any[output_arc("extra")],
            ),
        ],
    )
    @test adapter_reply(unreachable_state, load_env_request(unreachable_env; req_id=9)) == Dict(
        "kind" => "load_env_ok",
        "req_id" => 9,
    )
    @test adapter_reply(
        unreachable_state,
        run_env_request(
            req_id=10,
            env_run_id=11,
            initial_marking=Dict("ready" => Any[token_message()]),
        ),
    )["error"] |> x -> occursin("unreachable_transition:score", x)
end

@testset "adapter callback bridge" begin
    state = PevenPy.Adapter.AdapterState()
    @test adapter_reply(state, load_env_request(env_spec_message())) == Dict(
        "kind" => "load_env_ok",
        "req_id" => 1,
    )

    run_env = PevenPy.Adapter.accept_message!(
        state,
        run_env_request(
            req_id=2,
            env_run_id=7,
            initial_marking=Dict(
                "ready" => Any[token_message(payload=Dict("seed" => 7))],
            ),
        ),
    )
    @test run_env.reply == Dict("kind" => "run_env_ok", "req_id" => 2, "env_run_id" => 7)

    transport = FakeTransport(
        callback_replies=Any[
            Dict(
                "kind" => "callback_reply",
                "req_id" => 2,
                "env_run_id" => 7,
                "outputs" => Dict(
                    "done" => Any[token_message(run_key="rk-1", payload=Dict("seen" => 7))],
                ),
            ),
        ],
    )

    results = PevenPy.Adapter.execute_loaded_run!(state, run_env.accepted_run, transport)

    @test length(results) == 1
    @test only(results).status === :completed

    callback_request = only([
        message for message in transport.messages
        if get(message, "kind", nothing) == "callback_request"
    ])
    @test callback_request["transition_id"] == "finish"
    @test callback_request["req_id"] == 2
    @test callback_request["env_run_id"] == 7
    @test callback_request["bundle"] == Dict(
        "transition_id" => "finish",
        "run_key" => "rk-1",
        "selected_key" => nothing,
        "ordinal" => 1,
    )
    @test callback_request["attempt"] == 1
    @test length(callback_request["tokens"]) == 1
    @test callback_request["inputs_by_place"] == Dict(
        "ready" => Any[token_message(payload=Dict("seed" => 7))],
    )

    started = only([
        message for message in transport.messages
        if get(message, "kind", nothing) == "transition_started"
    ])
    @test started["inputs_by_place"] == Dict(
        "ready" => Any[token_message(payload=Dict("seed" => 7))],
    )
    @test any(message -> get(message, "kind", nothing) == "transition_completed", transport.messages)
    @test any(message -> get(message, "kind", nothing) == "run_finished", transport.messages)
end

@testset "adapter optional input callback bridge" begin
    optional_callback_env = env_spec_message(
        env_name="optional_callback_env",
        places=Any[place_spec("ready"), place_spec("plan"), place_spec("done")],
        transitions=Any[
            transition_spec(
                "finish";
                inputs=Any[input_arc("ready"), input_arc("plan"; optional=true)],
                outputs=Any[output_arc("done")],
            ),
        ],
    )

    function execute_optional_callback(initial_marking; env_run_id)
        state = PevenPy.Adapter.AdapterState()
        @test adapter_reply(state, load_env_request(optional_callback_env)) == Dict(
            "kind" => "load_env_ok",
            "req_id" => 1,
        )
        run_env = PevenPy.Adapter.accept_message!(
            state,
            run_env_request(
                req_id=2,
                env_run_id=env_run_id,
                initial_marking=initial_marking,
            ),
        )
        @test run_env.reply == Dict("kind" => "run_env_ok", "req_id" => 2, "env_run_id" => env_run_id)

        transport = FakeTransport(
            callback_replies=Any[
                Dict(
                    "kind" => "callback_reply",
                    "req_id" => 2,
                    "env_run_id" => env_run_id,
                    "outputs" => Dict("done" => Any[token_message(run_key="rk-1")]),
                ),
            ],
        )
        PevenPy.Adapter.execute_loaded_run!(state, run_env.accepted_run, transport)
        callback_request = only([
            message for message in transport.messages
            if get(message, "kind", nothing) == "callback_request"
        ])
        started = only([
            message for message in transport.messages
            if get(message, "kind", nothing) == "transition_started"
        ])
        return callback_request, started
    end

    absent_request, absent_started = execute_optional_callback(
        Dict("ready" => Any[token_message(payload=Dict("seed" => 12))]);
        env_run_id=12,
    )
    @test absent_request["inputs_by_place"]["ready"] == Any[token_message(payload=Dict("seed" => 12))]
    @test absent_request["inputs_by_place"]["plan"] == Any[]
    @test absent_started["inputs_by_place"]["ready"] == Any[token_message(payload=Dict("seed" => 12))]
    @test absent_started["inputs_by_place"]["plan"] == Any[]

    present_request, present_started = execute_optional_callback(
        Dict(
            "ready" => Any[token_message(payload=Dict("seed" => 13))],
            "plan" => Any[token_message(payload=Dict("plan" => "draft"))],
        );
        env_run_id=13,
    )
    @test present_request["inputs_by_place"]["ready"] == Any[token_message(payload=Dict("seed" => 13))]
    @test present_request["inputs_by_place"]["plan"] == Any[token_message(payload=Dict("plan" => "draft"))]
    @test present_started["inputs_by_place"]["ready"] == Any[token_message(payload=Dict("seed" => 13))]
    @test present_started["inputs_by_place"]["plan"] == Any[token_message(payload=Dict("plan" => "draft"))]
end

@testset "adapter callback errors fail the firing and finish the run" begin
    state = PevenPy.Adapter.AdapterState()
    @test adapter_reply(state, load_env_request(env_spec_message())) == Dict(
        "kind" => "load_env_ok",
        "req_id" => 1,
    )

    run_env = PevenPy.Adapter.accept_message!(
        state,
        run_env_request(
            req_id=2,
            env_run_id=8,
            initial_marking=Dict(
                "ready" => Any[token_message(payload=Dict("seed" => 8))],
            ),
        ),
    )
    @test run_env.reply == Dict("kind" => "run_env_ok", "req_id" => 2, "env_run_id" => 8)

    transport = FakeTransport(
        callback_replies=Any[
            Dict(
                "kind" => "callback_error",
                "req_id" => 2,
                "env_run_id" => 8,
                "error" => "callback boom",
            ),
        ],
    )

    results = PevenPy.Adapter.execute_loaded_run!(state, run_env.accepted_run, transport)

    @test length(results) == 1
    @test only(results).status === :failed
    @test only(results).error == "callback boom"

    failed = only([
        message for message in transport.messages
        if get(message, "kind", nothing) == "transition_failed"
    ])
    @test failed["error"] == "callback boom"
    @test failed["retrying"] === false

    finished = only([
        message for message in transport.messages
        if get(message, "kind", nothing) == "run_finished"
    ])
    @test finished["result"]["status"] == "failed"
    @test finished["result"]["error"] == "callback boom"
end

@testset "adapter protocol failures during callbacks poison the transport and abort the run" begin
    state = PevenPy.Adapter.AdapterState()
    @test adapter_reply(state, load_env_request(env_spec_message())) == Dict(
        "kind" => "load_env_ok",
        "req_id" => 1,
    )

    run_env = PevenPy.Adapter.accept_message!(
        state,
        run_env_request(
            req_id=2,
            env_run_id=9,
            initial_marking=Dict("ready" => Any[token_message(payload=Dict("seed" => 9))]),
        ),
    )
    @test run_env.reply == Dict("kind" => "run_env_ok", "req_id" => 2, "env_run_id" => 9)

    transport = FakeTransport(
        callback_replies=Any[
            Dict(
                "kind" => "callback_reply",
                "req_id" => 999,
                "env_run_id" => 9,
                "outputs" => Dict("done" => Any[token_message(run_key="rk-1")]),
            ),
        ],
    )

    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Adapter.execute_loaded_run!(
        state,
        run_env.accepted_run,
        transport,
    )
    @test transport.poisoned
    @test !isempty(transport.messages)
    @test !any(message -> get(message, "kind", nothing) == "run_finished", transport.messages)
end

@testset "adapter callback transport edge cases" begin
    state = PevenPy.Adapter.AdapterState()
    fused_env = env_spec_message(
        env_name="fused_env",
        places=Any[place_spec("ready"), place_spec("middle"), place_spec("done")],
        transitions=Any[
            transition_spec(
                "write";
                inputs=Any[input_arc("ready")],
                outputs=Any[output_arc("middle")],
                executor="write_executor",
            ),
            transition_spec(
                "read";
                inputs=Any[input_arc("middle")],
                outputs=Any[output_arc("done")],
                executor="read_executor",
            ),
        ],
    )
    @test adapter_reply(state, load_env_request(fused_env)) == Dict(
        "kind" => "load_env_ok",
        "req_id" => 1,
    )

    fused_run = PevenPy.Adapter.accept_message!(
        state,
        run_env_request(
            req_id=2,
            env_run_id=11,
            initial_marking=Dict("ready" => Any[token_message(payload=Dict("seed" => 11))]),
            fuse=1,
        ),
    )
    fused_transport = FakeTransport(
        callback_replies=Any[
            Dict(
                "kind" => "callback_reply",
                "req_id" => 2,
                "env_run_id" => 11,
                "outputs" => Dict("middle" => Any[token_message(run_key="rk-1", payload=Dict("seen" => 11))]),
            ),
        ],
    )
    fused_results = PevenPy.Adapter.execute_loaded_run!(state, fused_run.accepted_run, fused_transport)
    @test only(fused_results).terminal_reason === :fuse_exhausted

    missing_reply_transport = FakeTransport()
    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Adapter.execute_loaded_run!(
        state,
        fused_run.accepted_run,
        missing_reply_transport,
    )
    @test missing_reply_transport.poisoned

    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Adapter._decode_callback_reply(
        Dict(
            "kind" => "mystery",
            "req_id" => 2,
            "env_run_id" => 11,
        );
        req_id=2,
        env_run_id=11,
    )

    empty_state = PevenPy.Adapter.AdapterState()
    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Adapter.execute_loaded_run!(
        empty_state,
        PevenPy.Adapter.PreparedRun(1, Peven.Marking(Dict{Symbol,Vector{Peven.Token}}()), nothing),
        FakeTransport(),
    )
end

@testset "adapter runtime event write failures abort the run" begin
    state = PevenPy.Adapter.AdapterState()
    @test adapter_reply(state, load_env_request(env_spec_message())) == Dict(
        "kind" => "load_env_ok",
        "req_id" => 1,
    )

    run_env = PevenPy.Adapter.accept_message!(
        state,
        run_env_request(
            req_id=2,
            env_run_id=10,
            initial_marking=Dict("ready" => Any[token_message(payload=Dict("seed" => 10))]),
        ),
    )
    @test run_env.reply == Dict("kind" => "run_env_ok", "req_id" => 2, "env_run_id" => 10)

    transport = FakeTransport(
        callback_replies=Any[
            Dict(
                "kind" => "callback_reply",
                "req_id" => 2,
                "env_run_id" => 10,
                "outputs" => Dict("done" => Any[token_message(run_key="rk-1", payload=Dict("seen" => 10))]),
            ),
        ],
        fail_event_kind="transition_started",
    )

    @test_throws PevenPy.Protocol.ProtocolError PevenPy.Adapter.execute_loaded_run!(
        state,
        run_env.accepted_run,
        transport,
    )
    @test !any(message -> get(message, "kind", nothing) == "transition_started", transport.messages)
    @test !any(message -> get(message, "kind", nothing) == "run_finished", transport.messages)
end

@testset "entrypoint" begin
    @test_throws MethodError PevenPy.main("/tmp/peven.sock"; fail_event_kind="transition_started")
    @test !isdefined(PevenPy.Adapter, Symbol("handle_message!"))
end

@testset "engine-only validation classes not representable from authored IR" begin
    key_mismatch_net = Peven.Net(
        Dict(:ready => Peven.Place(:other)),
        Dict(:judge => Peven.Transition(:judge)),
        Peven.ArcFrom[],
        Peven.ArcTo[],
    )
    issues = Peven.validate(key_mismatch_net)
    @test any(issue -> issue.code === :key_mismatch && issue.object_id === :ready, issues)

    unknown_transition_net = Peven.Net(
        Dict(:ready => Peven.Place(:ready)),
        Dict(:judge => Peven.Transition(:judge)),
        [Peven.ArcFrom(:ghost, :ready)],
        Peven.ArcTo[],
    )
    issues = Peven.validate(unknown_transition_net)
    @test any(issue -> issue.code === :unknown_transition && issue.object_id === :ghost, issues)
end
