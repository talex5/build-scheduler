open Lwt.Infix

let src = Logs.Src.create "worker" ~doc:"build-scheduler worker agent"
module Log = (val Logs.src_log src : Logs.LOG)

module Log_data = struct
  let max_chunk_size = 10240L

  type t = {
    data : Buffer.t;
    mutable cond : [ `Running of unit Lwt_condition.t
                   | `Finished ]
  }
  
  let create () = 
    {
      data = Buffer.create 10240;
      cond = `Running (Lwt_condition.create ());
    }

  let rec stream t ~start =
    let len = Int64.of_int (Buffer.length t.data) in
    let start = if start < 0L then max 0L (Int64.add len start) else start in
    let avail = Int64.sub len start in
    if avail < 0L then Fmt.failwith "Start value out of range!";
    if avail = 0L then (
      match t.cond with
      | `Running cond ->
        Lwt_condition.wait cond >>= fun () ->
        stream t ~start
      | `Finished ->
        Lwt.return ("", start)
    ) else (
      let chunk = min avail max_chunk_size in
      let next = Int64.add start chunk in
      let start = Int64.to_int start in
      let avail = Int64.to_int avail in
      Lwt.return (Buffer.sub t.data start avail, next)
    )

  let write t data =
    match t.cond with
    | `Running cond ->
      Buffer.add_string t.data data;
      Lwt_condition.broadcast cond ()
    | `Finished ->
      Fmt.failwith "Attempt to write to log after close: %S" data

  let copy_from_stream t src =
    let rec aux () =
      Lwt_io.read ~count:4096 src >>= function
      | "" -> Lwt.return_unit
      | data -> write t data; aux ()
    in
    aux ()

  let close t =
    match t.cond with
    | `Running cond ->
      t.cond <- `Finished;
      Lwt_condition.broadcast cond ()
    | `Finished ->
      Fmt.failwith "Log already closed!"
end

let send_to ch contents =
  Lwt.try_bind
    (fun () ->
       Lwt_io.write ch contents >>= fun () ->
       Lwt_io.close ch
    )
    (fun () -> Lwt.return (Ok ()))
    (fun ex -> Lwt.return (Error (`Msg (Printexc.to_string ex))))

let pp_signal f x =
  let open Sys in
  if x = sigkill then Fmt.string f "kill"
  else if x = sigterm then Fmt.string f "term"
  else Fmt.int f x

type child_process = <
  status : Unix.process_status Lwt.t;
  stdin : Lwt_io.output Lwt_io.channel;
  stdout : Lwt_io.input Lwt_io.channel;
>

let build ~docker_build ~log { Api.Queue.dockerfile; cache_hint } =
  Log.info (fun f -> f "Got request to build (%s):@,%s" cache_hint dockerfile);
  let proc : child_process = docker_build () in
  let copy_thread = Log_data.copy_from_stream log proc#stdout in
  send_to proc#stdin dockerfile >>= fun stdin_result ->
  copy_thread >>= fun () -> (* Ensure all data has been copied before returning *)
  proc#status >|= function
  | Unix.WEXITED 0 ->
    begin match stdin_result with
      | Ok () ->
        Log_data.write log "Job succeeed\n";
        Ok ()
      | Error (`Msg msg) -> Fmt.failwith "Failed sending Dockerfile to process: %s" msg
    end
  | Unix.WEXITED x ->
    Log_data.write log (Fmt.strf "Docker build exited with status %d@." x);
    Error (`Msg "Build failed")
  | Unix.WSIGNALED x -> Fmt.failwith "Docker build failed with signal %d" x
  | Unix.WSTOPPED x -> Fmt.failwith "Docker build stopped with signal %a" pp_signal x

let docker_build () =
  (Lwt_process.open_process ~stderr:(`FD_copy Unix.stdout) ("", [| "docker"; "build"; "-" |]) :> child_process)

let run ?(docker_build=docker_build) ~capacity registration_service =
  let cond = Lwt_condition.create () in
  let in_use = ref 0 in
  let queue = Api.Registration.register registration_service ~name:"worker-1" in
  let rec loop () =
    if !in_use >= capacity then (
      Log.info (fun f -> f "At capacity. Waiting for a build to finish before requesting more...");
      Lwt_condition.wait cond >>= loop
    ) else (
      incr in_use;
      let outcome, set_outcome = Lwt.wait () in
      let log = Log_data.create () in
      Log.info (fun f -> f "Requesting a new job...");
      let job = Api.Job.local ~outcome ~stream_log_data:(Log_data.stream log) in
      Api.Queue.pop queue job >>= fun request ->
      Lwt.async (fun () ->
          Lwt.finalize
            (fun () ->
               Lwt.try_bind
                 (fun () -> build ~docker_build ~log request)
                 (fun outcome ->
                    Log_data.close log;
                    Lwt.wakeup set_outcome outcome;
                    Lwt.return_unit)
                 (fun ex ->
                    Log.warn (fun f -> f "Build failed: %a" Fmt.exn ex);
                    Log_data.write log (Fmt.strf "Uncaught exception: %a" Fmt.exn ex);
                    Log_data.close log;
                    Lwt.wakeup_exn set_outcome ex;
                    Lwt.return_unit)
            )
            (fun () -> decr in_use; Lwt_condition.broadcast cond (); Lwt.return_unit)
        );
      loop ()
    )
  in
  loop ()
