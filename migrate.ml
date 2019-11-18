open Printf

let md5 source = Digest.(to_hex (string source))

let (>>=) = Stdlib.Result.bind

let or_fail = Caqti_blocking.or_fail

let read_dir (dir: string): (string array, _) result =
  try Ok (Sys.readdir dir) with Sys_error e -> Error (`Dir e)

let read_file (name: string): (string, _) result =
  try Ok Stdio.In_channel.(with_file name ~f:input_all) with
  Sys_error e -> eprintf "%s\n" e; Error (`File e)

module Migration = struct
  type 'source t = {source: 'source; name: string; hash: string}


  module On_disc = struct
    type nonrec t = string t

    let of_ordered_list =
      List.map (fun (name, source) -> {source; name; hash=md5 source})
  end

  module In_db = struct
    type nonrec t = unit t

    let of_ordered_list =
      List.map (fun (name, hash) -> {source=(); name; hash})
  end

  module Plan = struct
    type t = {initialize: bool; apply: On_disc.t list}
  end

  let verify on_disc in_db =
    if on_disc.name <> in_db.name then
      Error (`Name_mismatch (on_disc.name, in_db.name))
    else if on_disc.hash <> on_disc.hash then
      Error (`Hash_mismatch (on_disc.hash, in_db.hash))
    else
      Ok ()

  let rec pending on_disc in_db =
    match on_disc, in_db with
    | x :: xs, y :: ys -> verify x y >>= fun () -> pending xs ys
    | [], in_db -> Error (`Db_is_ahead_of_sources (List.map (fun x -> x.name) in_db))
    | on_disc, [] -> Ok on_disc
end

module SQL = struct
  let query multiplicity output input source =
    Caqti_request.create_p input output multiplicity (fun _ -> source)

  let zero, one, zero_or_one, zero_or_more =
    Caqti_mult.(zero, one, zero_or_one, zero_or_more)

  let unit, bool, int, string, tup2 =
    Caqti_type.(unit, bool, int, string, tup2)
end

module Migrations = struct
  let table_exists =
    SQL.(query one bool unit)
    "select to_regclass('migrations') is not null"

  let create =
    SQL.(query zero unit unit) {|
      create table migrations (
        id serial primary key,
        name varchar not null,
        hash char(32) not null
      )
    |}

  let all =
    SQL.(query zero_or_more (tup2 string string) unit)
    "select name, hash from migrations order by id"

  let insert =
    SQL.(query zero unit (tup2 string string))
    "insert into migrations (name, hash) values ($1, $2)"
end


let plus =
  SQL.(query one int (tup2 int int))
  "select ?::integer + ?::integer"

let (let*) = Stdlib.Result.bind

let read_source_migrations dir =
  let* names = read_dir dir in
  Base.Array.sort ~compare:Base.String.compare names;
  let names = Base.Array.to_list names in
  Base.List.fold_result names ~init:[] ~f:(fun tail name ->
    let* source = read_file (dir ^ "/" ^ name) in
    Ok (Migration.{source; name; hash=md5 source} :: tail))

let read_db_migrations (module C: Caqti_blocking.CONNECTION) =
  let* exists = C.find Migrations.table_exists () in
  if not exists then Ok None else
  let* migrations = C.fold Migrations.all (fun (name, hash) tail ->
    Migration.{name; hash; source=()} :: tail) () [] in
  Ok (Some (List.rev migrations))


let main argv =
  match argv with
  | [_; uri; dir] ->
      let uri = Uri.of_string uri in
      let connection = Caqti_blocking.(connect uri |> or_fail) in
      let in_db = read_db_migrations connection |> or_fail |> Option.get in
      printf "db:\n";
      Base.List.iter in_db ~f:(fun {name; hash; _} -> printf "%s: %s\n" name hash);
      printf "disc:\n";
      let on_disc = read_source_migrations dir |> Stdlib.Result.get_ok in
      Base.List.iter on_disc ~f:(fun {name; hash; _} -> printf "%s: %s\n" name hash);
      printf "ok\n"
(*       printf "%s %d\n" Sys.ocaml_version (C.find plus (7, 13) |> or_fail); *)
(*       printf "=> %b\n" (C.find Migrations.table_exists () |> or_fail) *)
  | program :: _ -> eprintf "Usage: %s <uri> <dir>" program
  | _ -> assert false

let () = main (Array.to_list Sys.argv)

