module Tally.FreeDataAccess

open DataAccess
open Types



type DataAccess<'t, 'r> =
     | Get      of ('t -> 'r)
     | TryGet   of ('t option -> 'r)
     | Contains of (bool -> 'r)
     | Put      of ('t * 'r) 
     | Truncate of ('r)
     | Delete   of ('r)

type FreeDA<'t, 'a> =
     | Pure of 'a
     | Free of (Interval * DataAccess<'t, FreeDA<'t,'a>>)



module DataAccess =
    
     let map (f : 'a -> 'b) (da : DataAccess<'t,'a>) : DataAccess<'t,'b> =
          match da with
          | Get      g      -> Get (f << g)
          | TryGet   g      -> TryGet (f << g)
          | Contains (p)    -> Contains (f << p)
          | Put      (s, x) -> Put (s, f x)  
          | Truncate (x)    -> Truncate (f x)
          | Delete   (x)    -> Delete (f x)



let rec bind (f : 'a -> FreeDA<'t,'b>) (m : FreeDA<'t,'a>) : FreeDA<'t,'b> =
     match m with
     | Pure x             -> f x
     | Free (interval, t) -> Free (interval, DataAccess.map (bind f) t)

let ret = Pure

let join (mm : FreeDA<'t, FreeDA<'t,'a>>) : FreeDA<'t,'a> =
     bind id mm

let map (f : 'a -> 'b) (m : FreeDA<'t,'a>) : FreeDA<'t,'b> =
     bind (f >> ret) m

let liftDA (interval : Interval) (da : DataAccess<'t,'a>) : FreeDA<'t, 'a> =
    Free (interval, DataAccess.map Pure da)

type FreeDABuilder<'t>() =
    
    member this.Bind(m : FreeDA<'t,'a>, f : 'a -> FreeDA<'t,'b>) : FreeDA<'t,'b> =
         bind f m
    
    member this.Return(x : 'a) : FreeDA<'t,'a> =
         Pure x
    
    member this.Combine(m1 : FreeDA<'t,'a>, m2 : FreeDA<'t,'a>) : FreeDA<'t,'a> =
         m1 |> bind (fun _ -> m2)
    
    member this.Zero() : FreeDA<'t,unit> =
         Pure ()
    
    member this.Delay(f) : FreeDA<'t,'a> =
         f ()

let freeDA<'t> = new FreeDABuilder<'t>()

let inline (=<<) f m = bind f m

let inline (>>=) x f = bind f x

let inline (<*>) f m = f >>= (fun f -> m >>= (fun x -> f x))

let inline (<*|) f m = (<*>) f m

let inline (|*>) m f = f <*> m

let inline (<!>) f m = map f m

let inline (<@|) f m = map f m

let inline (|@>) m f = map f m

let inline (>=>) f g = fun x -> f x >>= g

let inline (<=<) g f = fun x -> f x >>= g



module Free =
     
     let get (interval : Interval) : FreeDA<'t,'t> =
          Get id |> liftDA interval
     
     let tryGet (interval : Interval) : FreeDA<'t,'t option> =
          TryGet id |> liftDA interval
     
     let contains (interval : Interval) : FreeDA<'t,bool> =
          Contains id |> liftDA interval
     
     let put (interval : Interval) (x : 't) : FreeDA<'t,unit> =
          Put (x, ()) |> liftDA interval
     
     let truncate (interval : Interval) : FreeDA<'t,unit> =
          Truncate () |> liftDA interval
     
     let delete (interval : Interval) : FreeDA<'t,unit> =
          Delete () |> liftDA interval



module Compute =
     
     open Consensus.Types
     
     module private Tip =
          module T = Tip
          type   T = Tip
          let rec compute (repo : DataAccess.T) (session : Session) (fda : FreeDA<T, 'r>) : 'r =
               let compute = compute repo session
               match fda with
               | Free (_, da) ->
                    match da with
                    | Get      g      -> T.get      repo session |> g |> compute
                    | TryGet   g      -> T.tryGet   repo session |> g |> compute
                    | Contains (_)    -> failwith "Tip.contains not implemented"
                    | Put      (s, x) -> T.put      repo session s ; compute x
                    | Truncate (_)    -> failwith "Tip.truncate not implemented"
                    | Delete   (_)    -> failwith "Tip.delete not implemented"
               | Pure x ->
                    x
     
     module private PKBalance =
          module T = PKBalance
          type   T = PKBalance
          let rec compute (repo : DataAccess.T) (session : Session) (fda : FreeDA<T, 'r>) : 'r =
               let compute = compute repo session
               match fda with
               | Free (interval, da) ->
                    match da with
                    | Get      g      -> T.get      repo session interval |> g |> compute
                    | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                    | Contains (p)    -> T.contains repo session interval |> p |> compute
                    | Put      (s, x) -> T.put      repo session interval s ; compute x
                    | Truncate (x)    -> T.truncate repo session            ; compute x
                    | Delete   (x)    -> T.delete   repo session interval   ; compute x
               | Pure x ->
                    x
     
     module private PKAllocation =
          module T = PKAllocation
          type   T = PKAllocation
          let rec compute (repo : DataAccess.T) (session : Session) (fda : FreeDA<T, 'r>) : 'r =
               let compute = compute repo session
               match fda with
               | Free (interval, da) ->
                    match da with
                    | Get      g      -> T.get      repo session interval |> g |> compute
                    | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                    | Contains (p)    -> T.contains repo session interval |> p |> compute
                    | Put      (s, x) -> T.put      repo session interval s ; compute x
                    | Truncate (x)    -> T.truncate repo session            ; compute x
                    | Delete   (x)    -> T.delete   repo session interval   ; compute x
               | Pure x ->
                    x
     
     module private VoteUtxoSet =
          module T = VoteUtxoSet
          type   T = VoteUtxo
          let rec compute (repo : DataAccess.T) (session : Session) (fda : FreeDA<T, 'r>) : 'r =
               let compute = compute repo session
               match fda with
               | Free (interval, da) ->
                    match da with
                    | Get      g      -> T.get      repo session interval |> g |> compute
                    | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                    | Contains (p)    -> T.contains repo session interval |> p |> compute
                    | Put      (s, x) -> T.put      repo session interval s ; compute x
                    | Truncate (x)    -> T.truncate repo session            ; compute x
                    | Delete   (x)    -> T.delete   repo session interval   ; compute x
               | Pure x ->
                    x
     
     module private PKPayout =
          module T = PKPayout
          type   T = PKPayout
          let rec compute (repo : DataAccess.T) (session : Session) (fda : FreeDA<T, 'r>) : 'r =
               let compute = compute repo session
               match fda with
               | Free (interval, da) ->
                    match da with
                    | Get      g      -> T.get      repo session interval |> g |> compute
                    | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                    | Contains (p)    -> T.contains repo session interval |> p |> compute
                    | Put      (s, x) -> T.put      repo session interval s ; compute x
                    | Truncate (x)    -> T.truncate repo session            ; compute x
                    | Delete   (x)    -> T.delete   repo session interval   ; compute x
               | Pure x ->
                    x
     
     module private Fund =
          module T = Fund
          type   T = Fund
          let rec compute (repo : DataAccess.T) (session : Session) (fda : FreeDA<T, 'r>) : 'r =
               let compute = compute repo session
               match fda with
               | Free (interval, da) ->
                    match da with
                    | Get      g      -> T.get      repo session interval |> g |> compute
                    | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                    | Contains (p)    -> T.contains repo session interval |> p |> compute
                    | Put      (s, x) -> T.put      repo session interval s ; compute x
                    | Truncate (x)    -> T.truncate repo session            ; compute x
                    | Delete   (x)    -> T.delete   repo session interval   ; compute x
               | Pure x ->
                    x
     
     module private Winner =
          module T = Winner
          type   T = Winner
          let rec compute (repo : DataAccess.T) (session : Session) (fda : FreeDA<T, 'r>) : 'r =
               let compute = compute repo session
               match fda with
               | Free (interval, da) ->
                    match da with
                    | Get      g      -> T.get      repo session interval |> g |> compute
                    | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                    | Contains (p)    -> T.contains repo session interval |> p |> compute
                    | Put      (s, x) -> T.put      repo session interval s ; compute x
                    | Truncate (x)    -> T.truncate repo session            ; compute x
                    | Delete   (x)    -> T.delete   repo session interval   ; compute x
               | Pure x ->
                    x

     let Tip          = Tip          .compute
     let PKBalance    = PKBalance    .compute
     let PKAllocation = PKAllocation .compute
     let VoteUtxoSet  = VoteUtxoSet  .compute
     let PKPayout     = PKPayout     .compute
     let Fund         = Fund         .compute
     let Winner       = Winner       .compute
