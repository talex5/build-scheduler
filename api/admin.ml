open Lwt.Infix
open Capnp_rpc_lwt

let local ~pools ~pool ~get_active ~set_active =
  let module X = Raw.Service.Admin in
  X.local @@ object
    inherit X.service

    method pools_impl _params release_param_caps =
      let open X.Pools in
      release_param_caps ();
      let response, results = Service.Response.create Results.init_pointer in
      Results.names_set_list results (pools ()) |> ignore;
      Service.return response

    method pool_impl params release_param_caps =
      let open X.Pool in
      let name = Params.name_get params in
      release_param_caps ();
      let response, results = Service.Response.create Results.init_pointer in
      let cap = pool name in
      Results.pool_set results (Some cap);
      Capability.dec_ref cap;
      Service.return response

    method get_active_impl _params release_param_caps =
      let open X.GetActive in
      release_param_caps ();
      let response, results = Service.Response.create Results.init_pointer in
      Results.active_set results (get_active ());
      Service.return response

    method set_active_impl params release_param_caps =
      let open X.SetActive in
      let active = Params.active_get params in
      release_param_caps ();
      set_active active;
      Service.return_empty ()
  end

module X = Raw.Client.Admin

type t = X.t Capability.t

let pools t =
  let open X.Pools in
  let request = Capability.Request.create_no_args () in
  Capability.call_for_value_exn t method_id request >|= fun results ->
  Results.names_get_list results

let pool t name =
  let open X.Pool in
  let request, params = Capability.Request.create Params.init_pointer in
  Params.name_set params name;
  Capability.call_for_caps t method_id request Results.pool_get_pipelined

let set_active t active =
  let open X.SetActive in
  let request, params = Capability.Request.create Params.init_pointer in
  Params.active_set params active;
  Capability.call_for_unit_exn t method_id request
