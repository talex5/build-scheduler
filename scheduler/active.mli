type t

val create : unit -> t
val set : t -> bool -> unit
val get : t -> bool

val await : t -> unit Lwt.t
(** [await t] returns once [t] is active. *)
