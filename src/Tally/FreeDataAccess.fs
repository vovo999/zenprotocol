module Tally.FreeDataAccess

open DataAccess
open Types



type Accessor<'t, 'r> =
     | Get      of ('t -> 'r)
     | TryGet   of ('t option -> 'r)
     | Contains of (bool -> 'r)
     | Put      of ('t * 'r) 
     | Truncate of ('r)
     | Delete   of ('r)

module Accessor =
    
     let map (f : 'a -> 'b) (acc : Accessor<'t,'a>) : Accessor<'t,'b> =
          match acc with
          | Get      g      -> Get (f << g)
          | TryGet   g      -> TryGet (f << g)
          | Contains (p)    -> Contains (f << p)
          | Put      (s, x) -> Put (s, f x)  
          | Truncate (x)    -> Truncate (f x)
          | Delete   (x)    -> Delete (f x)

type FreeAccessor<'t, 'a> =
     | Pure of 'a
     | Free of (Interval * Accessor<'t, FreeAccessor<'t,'a>>)

let rec bind (f : 'a -> FreeAccessor<'t,'b>) (m : FreeAccessor<'t,'a>) : FreeAccessor<'t,'b> =
     match m with
     | Pure x             -> f x
     | Free (interval, t) -> Free (interval, Accessor.map (bind f) t)

let ret = Pure

let join (mm : FreeAccessor<'t, FreeAccessor<'t,'a>>) : FreeAccessor<'t,'a> =
     bind id mm

let map (f : 'a -> 'b) (m : FreeAccessor<'t,'a>) : FreeAccessor<'t,'b> =
     bind (f >> ret) m

let liftAccessor (interval : Interval) (acc : Accessor<'t,'a>) : FreeAccessor<'t, 'a> =
    Free (interval, Accessor.map Pure acc)

type FreeAccessorBuilder<'t>() =
    
     member this.Bind(m : FreeAccessor<'t,'a>, f : 'a -> FreeAccessor<'t,'b>) : FreeAccessor<'t,'b> =
          bind f m
     
     member this.Return(x : 'a) : FreeAccessor<'t,'a> =
          Pure x
     
     member this.ReturnFrom(x : FreeAccessor<'t,'a>) : FreeAccessor<'t,'a> =
          x
     
     member this.Combine(m1 : FreeAccessor<'t,'a>, m2 : FreeAccessor<'t,'a>) : FreeAccessor<'t,'a> =
          m1 |> bind (fun _ -> m2)
     
     member this.Zero() : FreeAccessor<'t,unit> =
          Pure ()
     
     member this.Delay(f) : FreeAccessor<'t,'a> =
          f ()

let freeAccessor<'t> = new FreeAccessorBuilder<'t>()

module Operators =
     
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
     
     let get interval : FreeAccessor<'t, 't> =
          Get id |> liftAccessor interval
     
     let tryGet interval : FreeAccessor<'t, 't option> =
          TryGet id |> liftAccessor interval
     
     let contains interval : FreeAccessor<'t, bool> =
          Contains id |> liftAccessor interval
     
     let put interval (x : 't) : FreeAccessor<'t, unit> =
          Put (x, ()) |> liftAccessor interval
     
     let truncate interval : FreeAccessor<'t, unit> =
          Truncate () |> liftAccessor interval
     
     let delete interval : FreeAccessor<'t, unit> =
          Delete () |> liftAccessor interval



module Compute =
     
     open Consensus.Types
     
     module private Tip =
          module T = Tip
          type   T = Tip
          let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<T, 'r>) : 'r =
               let compute = compute repo session
               match m with
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
          let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<T, 'r>) : 'r =
               let compute = compute repo session
               match m with
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
          let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<T, 'r>) : 'r =
               let compute = compute repo session
               match m with
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
          let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<T, 'r>) : 'r =
               let compute = compute repo session
               match m with
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
          let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<T, 'r>) : 'r =
               let compute = compute repo session
               match m with
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
          let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<T, 'r>) : 'r =
               let compute = compute repo session
               match m with
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
          let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<T, 'r>) : 'r =
               let compute = compute repo session
               match m with
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

module FreeDA =
     
     type AbstractDataAcces<'a> =
          | Tip          of Accessor<Tip, 'a>
          | PKBalance    of Accessor<PKBalance, 'a>
          | PKAllocation of Accessor<PKAllocation, 'a>
          | VoteUtxo     of Accessor<VoteUtxo, 'a>
          | PKPayout     of Accessor<PKPayout, 'a>
          | Fund         of Accessor<Fund, 'a>
          | Winner       of Accessor<Consensus.Types.Winner, 'a>
     
     module AbstractDataAcces =
          
          let map (f : 'a -> 'b) (ada : AbstractDataAcces<'a>) : AbstractDataAcces<'b> =
               match ada with
               | Tip          acc -> Tip          <| Accessor.map f acc    
               | PKBalance    acc -> PKBalance    <| Accessor.map f acc          
               | PKAllocation acc -> PKAllocation <| Accessor.map f acc             
               | VoteUtxo     acc -> VoteUtxo     <| Accessor.map f acc         
               | PKPayout     acc -> PKPayout     <| Accessor.map f acc         
               | Fund         acc -> Fund         <| Accessor.map f acc     
               | Winner       acc -> Winner       <| Accessor.map f acc       
     
     type FreeDA<'a> =
          | PureDA of 'a
          | FreeDA of (Interval * AbstractDataAcces<FreeDA<'a>>)
     
     let rec bind (f : 'a -> FreeDA<'b>) (m : FreeDA<'a>) : FreeDA<'b> =
          match m with
          | PureDA x             -> f x
          | FreeDA (interval, t) -> FreeDA (interval, AbstractDataAcces.map (bind f) t)
     
     let ret = PureDA
     
     let join (mm : FreeDA<FreeDA<'a>>) : FreeDA<'a> =
          bind id mm
     
     let map (f : 'a -> 'b) (m : FreeDA<'a>) : FreeDA<'b> =
          bind (f >> ret) m
     
     let liftADA (interval : Interval) (acc : AbstractDataAcces<'a>) : FreeDA<'a> =
         FreeDA (interval, AbstractDataAcces.map PureDA acc)
     
     module Lift =
          let Tip          interval = Tip          >> liftADA interval
          let PKBalance    interval = PKBalance    >> liftADA interval
          let PKAllocation interval = PKAllocation >> liftADA interval
          let VoteUtxo     interval = VoteUtxo     >> liftADA interval
          let PKPayout     interval = PKPayout     >> liftADA interval
          let Fund         interval = Fund         >> liftADA interval
          let Winner       interval = Winner       >> liftADA interval
     
     type FreeDABuilder() =
     
          member this.Bind(m : FreeDA<'a>, f : 'a -> FreeDA<'b>) : FreeDA<'b> =
               bind f m
          
          member this.Return(x : 'a) : FreeDA<'a> =
               PureDA x
          
          member this.ReturnFrom(x : FreeDA<'a>) : FreeDA<'a> =
               x

          member this.Combine(m1 : FreeDA<'a>, m2 : FreeDA<'a>) : FreeDA<'a> =
               m1 |> bind (fun _ -> m2)
          
          member this.Zero() : FreeDA<unit> =
               PureDA ()

          member this.Delay(f) : FreeDA<'a> =
               f ()
     
     let freeDA = new FreeDABuilder()
     
     module Operators =
     
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