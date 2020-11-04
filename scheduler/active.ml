type state =
  | Active
  | Paused of unit Lwt.t * unit Lwt.u

type t = state ref

let create () = ref Active

let set t v =
  match !t, v with
  | Active, true
  | Paused _, false -> ()
  | Active, false ->
    let (ready, set_ready) = Lwt.wait () in
    t := Paused (ready, set_ready)
  | Paused (_, set_ready), true ->
    t := Active;
    Lwt.wakeup set_ready ()

let get t =
  match !t with
  | Active -> true
  | Paused _ -> false

let await t =
  match !t with
  | Active -> Lwt.return_unit
  | Paused (ready, _) -> ready
