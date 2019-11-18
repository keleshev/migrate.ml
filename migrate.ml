open Printf

let md5 source = Digest.(to_hex (string source))

let ok = Stdlib.Result.ok

let or_fail = Caqti_blocking.or_fail

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

  let same on_disc in_db =
    on_disc.name = in_db.name && on_disc.hash = in_db.hash

  let rec compare_many on_disc in_db =
    match on_disc, in_db with
    | x :: xs, y :: ys ->
        if same x y then compare_many xs ys else Error `Conflicting_migrations
    | [], _ :: _ -> Error `Db_is_ahead_of_sources
    | migrations, [] -> Ok (`Apply migrations)
end

module SQL = struct
  let query multiplicity input output source =
    Caqti_request.create_p input output multiplicity (fun _ -> source)

  let zero, one, zero_or_one, zero_or_more =
    Caqti_mult.(zero, one, zero_or_one, zero_or_more)

  let unit, bool, int, string, tup2 =
    Caqti_type.(unit, bool, int, string, tup2)
end

module Migrations = struct
  let table_exists =
    SQL.(query one unit bool)
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
    SQL.(query zero_or_more unit (tup2 string string))
    "select name, hash from migrations order by id"

  let insert =
    SQL.(query zero (tup2 string string) unit)
    "insert into migrations (name, hash) values ($1, $2)"
end


let plus =
  SQL.(query one (tup2 int int) int)
  "select ?::integer + ?::integer"

let main argv =
  match argv with
  | [program] -> eprintf "Usage: %s <uri>" program
  | [_; uri] ->
      let uri = Uri.of_string uri in
      let module C = (val Caqti_blocking.(connect uri |> or_fail)) in
      printf "%s %d\n" Sys.ocaml_version (C.find plus (7, 13) |> or_fail);
      printf "=> %b\n" (C.find Migrations.table_exists () |> or_fail)
  | _ -> assert false

let () = main (Array.to_list Sys.argv)

