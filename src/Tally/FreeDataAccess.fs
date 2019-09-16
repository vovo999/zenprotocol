module Tally.FreeDataAccess

open DataAccess
open Types



module FreeAccessor = 

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
     
     type FreeAccessor<'env, 't, 'a> =
          | PureAcc of 'a
          | FreeAcc of ('env * Accessor<'t, FreeAccessor<'env,'t,'a>>)
     
     let rec bind (f : 'a -> FreeAccessor<'e, 't,'b>) (m : FreeAccessor<'e,'t,'a>) : FreeAccessor<'e,'t,'b> =
          match m with
          | PureAcc x             -> f x
          | FreeAcc (interval, t) -> FreeAcc (interval, Accessor.map (bind f) t)
     
     let ret = PureAcc
     
     let join (mm : FreeAccessor<'e, 't, FreeAccessor<'e,'t,'a>>) : FreeAccessor<'e,'t,'a> =
          bind id mm
     
     let map (f : 'a -> 'b) (m : FreeAccessor<'e,'t,'a>) : FreeAccessor<'e,'t,'b> =
          bind (f >> ret) m
     
     let liftAccessor (env : 'e) (acc : Accessor<'t,'a>) : FreeAccessor<'e, 't, 'a> =
         FreeAcc (env, Accessor.map PureAcc acc)
     
     type FreeAccessorBuilder<'t>() =
         
          member this.Bind(m : FreeAccessor<'e,'t,'a>, f : 'a -> FreeAccessor<'e,'t,'b>) : FreeAccessor<'e,'t,'b> =
               bind f m
          
          member this.Return(x : 'a) : FreeAccessor<'e,'t,'a> =
               PureAcc x
          
          member this.ReturnFrom(x : FreeAccessor<'e,'t,'a>) : FreeAccessor<'e,'t,'a> =
               x
          
          member this.Combine(m1 : FreeAccessor<'e,'t,'a>, m2 : FreeAccessor<'e,'t,'a>) : FreeAccessor<'e,'t,'a> =
               m1 |> bind (fun _ -> m2)
          
          member this.Zero() : FreeAccessor<'e,'t,unit> =
               PureAcc ()
          
          member this.Delay(f) : FreeAccessor<'e,'t,'a> =
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
          
          let get interval : FreeAccessor<'e, 't, 't> =
               Get id |> liftAccessor interval
          
          let tryGet interval : FreeAccessor<'e, 't, 't option> =
               TryGet id |> liftAccessor interval
          
          let contains interval : FreeAccessor<'e, 't, bool> =
               Contains id |> liftAccessor interval
          
          let put interval (x : 't) : FreeAccessor<'e, 't, unit> =
               Put (x, ()) |> liftAccessor interval
          
          let truncate interval : FreeAccessor<'e, 't, unit> =
               Truncate () |> liftAccessor interval
          
          let delete interval : FreeAccessor<'e, 't, unit> =
               Delete () |> liftAccessor interval
     
     module Compute =
          
          open Consensus.Types
          
          module private Tip =
               module T = Tip
               type   T = Tip
               let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<unit, T, 'r>) : 'r =
                    let compute = compute repo session
                    match m with
                    | FreeAcc ((), acc) ->
                         match acc with
                         | Get      g      -> T.get      repo session |> g |> compute
                         | TryGet   g      -> T.tryGet   repo session |> g |> compute
                         | Contains (_)    -> failwith "Tip.contains not implemented"
                         | Put      (s, x) -> T.put      repo session s ; compute x
                         | Truncate (_)    -> failwith "Tip.truncate not implemented"
                         | Delete   (_)    -> failwith "Tip.delete not implemented"
                    | PureAcc x ->
                         x
          
          module private PKBalance =
               module T = PKBalance
               type   T = PKBalance
               let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<Interval, T, 'r>) : 'r =
                    let compute = compute repo session
                    match m with
                    | FreeAcc (interval, acc) ->
                         match acc with
                         | Get      g      -> T.get      repo session interval |> g |> compute
                         | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                         | Contains (p)    -> T.contains repo session interval |> p |> compute
                         | Put      (s, x) -> T.put      repo session interval s ; compute x
                         | Truncate (x)    -> T.truncate repo session            ; compute x
                         | Delete   (x)    -> T.delete   repo session interval   ; compute x
                    | PureAcc x ->
                         x
          
          module private PKAllocation =
               module T = PKAllocation
               type   T = PKAllocation
               let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<Interval, T, 'r>) : 'r =
                    let compute = compute repo session
                    match m with
                    | FreeAcc (interval, acc) ->
                         match acc with
                         | Get      g      -> T.get      repo session interval |> g |> compute
                         | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                         | Contains (p)    -> T.contains repo session interval |> p |> compute
                         | Put      (s, x) -> T.put      repo session interval s ; compute x
                         | Truncate (x)    -> T.truncate repo session            ; compute x
                         | Delete   (x)    -> T.delete   repo session interval   ; compute x
                    | PureAcc x ->
                         x
          
          module private VoteUtxoSet =
               module T = VoteUtxoSet
               type   T = VoteUtxo
               let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<Interval, T, 'r>) : 'r =
                    let compute = compute repo session
                    match m with
                    | FreeAcc (interval, acc) ->
                         match acc with
                         | Get      g      -> T.get      repo session interval |> g |> compute
                         | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                         | Contains (p)    -> T.contains repo session interval |> p |> compute
                         | Put      (s, x) -> T.put      repo session interval s ; compute x
                         | Truncate (x)    -> T.truncate repo session            ; compute x
                         | Delete   (x)    -> T.delete   repo session interval   ; compute x
                    | PureAcc x ->
                         x
          
          module private PKPayout =
               module T = PKPayout
               type   T = PKPayout
               let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<Interval, T, 'r>) : 'r =
                    let compute = compute repo session
                    match m with
                    | FreeAcc (interval, acc) ->
                         match acc with
                         | Get      g      -> T.get      repo session interval |> g |> compute
                         | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                         | Contains (p)    -> T.contains repo session interval |> p |> compute
                         | Put      (s, x) -> T.put      repo session interval s ; compute x
                         | Truncate (x)    -> T.truncate repo session            ; compute x
                         | Delete   (x)    -> T.delete   repo session interval   ; compute x
                    | PureAcc x ->
                         x
          
          module private Fund =
               module T = Fund
               type   T = Fund
               let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<Interval, T, 'r>) : 'r =
                    let compute = compute repo session
                    match m with
                    | FreeAcc (interval, acc) ->
                         match acc with
                         | Get      g      -> T.get      repo session interval |> g |> compute
                         | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                         | Contains (p)    -> T.contains repo session interval |> p |> compute
                         | Put      (s, x) -> T.put      repo session interval s ; compute x
                         | Truncate (x)    -> T.truncate repo session            ; compute x
                         | Delete   (x)    -> T.delete   repo session interval   ; compute x
                    | PureAcc x ->
                         x
          
          module private Winner =
               module T = Winner
               type   T = Winner
               let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<Interval, T, 'r>) : 'r =
                    let compute = compute repo session
                    match m with
                    | FreeAcc (interval, acc) ->
                         match acc with
                         | Get      g      -> T.get      repo session interval |> g |> compute
                         | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                         | Contains (p)    -> T.contains repo session interval |> p |> compute
                         | Put      (s, x) -> T.put      repo session interval s ; compute x
                         | Truncate (x)    -> T.truncate repo session            ; compute x
                         | Delete   (x)    -> T.delete   repo session interval   ; compute x
                    | PureAcc x ->
                         x
          
          module private Allocation =
               module T = Allocation
               type   T = Allocation
               let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<Interval, T, 'r>) : 'r =
                    let compute = compute repo session
                    match m with
                    | FreeAcc (interval, acc) ->
                         match acc with
                         | Get      g      -> T.get      repo session interval |> g |> compute
                         | TryGet   g      -> T.tryGet   repo session interval |> g |> compute
                         | Contains (p)    -> T.contains repo session interval |> p |> compute
                         | Put      (s, x) -> T.put      repo session interval s ; compute x
                         | Truncate (x)    -> T.truncate repo session            ; compute x
                         | Delete   (x)    -> T.delete   repo session interval   ; compute x
                    | PureAcc x ->
                         x
          
          let Tip          = Tip          .compute
          let PKBalance    = PKBalance    .compute
          let PKAllocation = PKAllocation .compute
          let VoteUtxoSet  = VoteUtxoSet  .compute
          let PKPayout     = PKPayout     .compute
          let Fund         = Fund         .compute
          let Winner       = Winner       .compute
          let Allocation   = Allocation   .compute

type AbstractDataAcces<'a> =
     | Tip          of FreeAccessor.FreeAccessor<unit, Tip, 'a>
     | PKBalance    of FreeAccessor.FreeAccessor<Interval, PKBalance, 'a>
     | PKAllocation of FreeAccessor.FreeAccessor<Interval, PKAllocation, 'a>
     | VoteUtxo     of FreeAccessor.FreeAccessor<Interval, VoteUtxo, 'a>
     | PKPayout     of FreeAccessor.FreeAccessor<Interval, PKPayout, 'a>
     | Fund         of FreeAccessor.FreeAccessor<Interval, Fund, 'a>
     | Winner       of FreeAccessor.FreeAccessor<Interval, Consensus.Types.Winner, 'a>
     | Allocation   of FreeAccessor.FreeAccessor<Interval, Tally.Types.Allocation, 'a>

module AbstractDataAcces =
     
     let map (f : 'a -> 'b) (ada : AbstractDataAcces<'a>) : AbstractDataAcces<'b> =
          match ada with
          | Tip          acc -> Tip          <| FreeAccessor.map f acc
          | PKBalance    acc -> PKBalance    <| FreeAccessor.map f acc
          | PKAllocation acc -> PKAllocation <| FreeAccessor.map f acc
          | VoteUtxo     acc -> VoteUtxo     <| FreeAccessor.map f acc
          | PKPayout     acc -> PKPayout     <| FreeAccessor.map f acc
          | Fund         acc -> Fund         <| FreeAccessor.map f acc
          | Winner       acc -> Winner       <| FreeAccessor.map f acc
          | Allocation   acc -> Allocation   <| FreeAccessor.map f acc

type FreeDA<'a> =
     | PureDA of 'a
     | FreeDA of AbstractDataAcces<FreeDA<'a>>

let rec bind (f : 'a -> FreeDA<'b>) (m : FreeDA<'a>) : FreeDA<'b> =
     match m with
     | PureDA x -> f x
     | FreeDA t -> FreeDA (AbstractDataAcces.map (bind f) t)

let ret = PureDA

let join (mm : FreeDA<FreeDA<'a>>) : FreeDA<'a> =
     bind id mm

let map (f : 'a -> 'b) (m : FreeDA<'a>) : FreeDA<'b> =
     bind (f >> ret) m

let liftADA (acc : AbstractDataAcces<'a>) : FreeDA<'a> =
    FreeDA (AbstractDataAcces.map PureDA acc)

let nothing = ret ()

module Lift =
     let Tip          acc = acc |> Tip          |> liftADA
     let PKBalance    acc = acc |> PKBalance    |> liftADA
     let PKAllocation acc = acc |> PKAllocation |> liftADA
     let VoteUtxo     acc = acc |> VoteUtxo     |> liftADA
     let PKPayout     acc = acc |> PKPayout     |> liftADA
     let Fund         acc = acc |> Fund         |> liftADA
     let Winner       acc = acc |> Winner       |> liftADA
     let Allocation   acc = acc |> Allocation   |> liftADA

type FreeDABuilder() =

     member this.Bind(m : FreeDA<'a>, f : 'a -> FreeDA<'b>) : FreeDA<'b> =
          bind f m
     
     member this.Return(x : 'a) : FreeDA<'a> =
          ret x
     
     member this.ReturnFrom(x : FreeDA<'a>) : FreeDA<'a> =
          x

     member this.Combine(m1 : FreeDA<'a>, m2 : FreeDA<'a>) : FreeDA<'a> =
          m1 |> bind (fun _ -> m2)
     
     member this.Zero() : FreeDA<unit> =
          nothing

     member this.Delay(f) : FreeDA<'a> =
          f ()

let freeDA = new FreeDABuilder()

module Operators =

     let inline (=<<) f m = bind f m
     
     let inline (>>=) x f = bind f x
     
     let inline (<*>) f m = f >>= (fun f -> m >>= (fun x -> ret (f x)))
     
     let inline (<*|) f m = (<*>) f m
     
     let inline (|*>) m f = f <*> m
     
     let inline (<!>) f m = map f m
     
     let inline (<@|) f m = map f m
     
     let inline (|@>) m f = map f m
     
     let inline (>=>) f g = fun x -> f x >>= g
     
     let inline (<=<) g f = fun x -> f x >>= g

open Operators

let inline ap f m = f <*> m

let inline lift0 x = ret x 

let inline lift1 f x = f <!> x

let inline lift2 f x y = f <!> x <*> y

let inline lift3 f x y z = f <!> x <*> y <*> z

let inline lift4 f x y z w = f <!> x <*> y <*> z <*> w

let foldM (f : 'state -> 't -> FreeDA<'state>) (s : 'state) : seq<'t> -> FreeDA<'state> = 
     Seq.fold (fun acc t -> acc >>= fun y -> f y t) (ret s)

let rec sequence (xs : FreeDA<'a> list) : FreeDA<'a list> =
     let cons x xs = x::xs
     match xs with
     | [] ->
          ret []
     | hd :: tl ->
          cons <!> hd <*> sequence tl 

let inline mapM f x = sequence (List.map f x)

let iter (f : 'a -> FreeDA<unit>) (xs : 'a list) : FreeDA<unit> =
     mapM f xs >>= (fun _ -> nothing)

module Compute =
     
     let (<!>) = FreeAccessor.map
     
     let rec compute (repo : DataAccess.T) (session : Session) (data : FreeDA<'a>) : 'a =
          let compute = compute repo session
          match data with
          | FreeDA da ->
               match da with
               | Tip          fa -> compute <!> fa |> FreeAccessor.Compute.Tip          repo session
               | PKBalance    fa -> compute <!> fa |> FreeAccessor.Compute.PKBalance    repo session          
               | PKAllocation fa -> compute <!> fa |> FreeAccessor.Compute.PKAllocation repo session             
               | VoteUtxo     fa -> compute <!> fa |> FreeAccessor.Compute.VoteUtxoSet  repo session         
               | PKPayout     fa -> compute <!> fa |> FreeAccessor.Compute.PKPayout     repo session         
               | Fund         fa -> compute <!> fa |> FreeAccessor.Compute.Fund         repo session     
               | Winner       fa -> compute <!> fa |> FreeAccessor.Compute.Winner       repo session
               | Allocation   fa -> compute <!> fa |> FreeAccessor.Compute.Allocation   repo session
          | PureDA x ->
               x

module Free =
     
     module Tip =
          type T = Tip
          let  K = Tip
           
          let lift (acc : FreeAccessor.FreeAccessor<unit, T,'a>) : FreeDA<'a> =
               acc |> K |> liftADA 
          
          let get : FreeDA<T> =
               FreeAccessor.Free.get ()
               |> lift
          
          let tryGet : FreeDA<T option> =
               FreeAccessor.Free.tryGet ()
               |> lift
          
          let contains : FreeDA<bool> =
               FreeAccessor.Free.contains ()
               |> lift
          
          let put (x : T) : FreeDA<unit> =
               FreeAccessor.Free.put () x
               |> lift
          
          let truncate : FreeDA<unit> =
               FreeAccessor.Free.truncate ()
               |> lift
          
          let delete : FreeDA<unit> =
               FreeAccessor.Free.delete ()
               |> lift
               
     module PKBalance =
          type T = PKBalance
          let  K = PKBalance
          
          let lift (acc : FreeAccessor.FreeAccessor<Interval, T,'a>) : FreeDA<'a> =
               acc |> K |> liftADA 
          
          let get interval : FreeDA<T> =
               FreeAccessor.Free.get interval
               |> lift
          
          let tryGet interval : FreeDA<T option> =
               FreeAccessor.Free.tryGet interval
               |> lift
          
          let contains interval : FreeDA<bool> =
               FreeAccessor.Free.contains interval
               |> lift
          
          let put interval (x : T) : FreeDA<unit> =
               FreeAccessor.Free.put interval x
               |> lift
          
          let truncate interval : FreeDA<unit> =
               FreeAccessor.Free.truncate interval
               |> lift
          
          let delete interval : FreeDA<unit> =
               FreeAccessor.Free.delete interval
               |> lift
     
     module PKAllocation =
          type T = PKAllocation
          let  K = PKAllocation
          
          let lift (acc : FreeAccessor.FreeAccessor<Interval, T,'a>) : FreeDA<'a> =
               acc |> K |> liftADA 
          
          let get interval : FreeDA<T> =
               FreeAccessor.Free.get interval
               |> lift
          
          let tryGet interval : FreeDA<T option> =
               FreeAccessor.Free.tryGet interval
               |> lift
          
          let contains interval : FreeDA<bool> =
               FreeAccessor.Free.contains interval
               |> lift
          
          let put interval (x : T) : FreeDA<unit> =
               FreeAccessor.Free.put interval x
               |> lift
          
          let truncate interval : FreeDA<unit> =
               FreeAccessor.Free.truncate interval
               |> lift
          
          let delete interval : FreeDA<unit> =
               FreeAccessor.Free.delete interval
               |> lift
     
     module VoteUtxo =
          type T = VoteUtxo
          let  K = VoteUtxo
          
          let lift (acc : FreeAccessor.FreeAccessor<Interval, T,'a>) : FreeDA<'a> =
               acc |> K |> liftADA 
          
          let get interval : FreeDA<T> =
               FreeAccessor.Free.get interval
               |> lift
          
          let tryGet interval : FreeDA<T option> =
               FreeAccessor.Free.tryGet interval
               |> lift
          
          let contains interval : FreeDA<bool> =
               FreeAccessor.Free.contains interval
               |> lift
          
          let put interval (x : T) : FreeDA<unit> =
               FreeAccessor.Free.put interval x
               |> lift
          
          let truncate interval : FreeDA<unit> =
               FreeAccessor.Free.truncate interval
               |> lift
          
          let delete interval : FreeDA<unit> =
               FreeAccessor.Free.delete interval
               |> lift
     
     module PKPayout =
          type T = PKPayout
          let  K = PKPayout
          
          let lift (acc : FreeAccessor.FreeAccessor<Interval, T,'a>) : FreeDA<'a> =
               acc |> K |> liftADA 
          
          let get interval : FreeDA<T> =
               FreeAccessor.Free.get interval
               |> lift
          
          let tryGet interval : FreeDA<T option> =
               FreeAccessor.Free.tryGet interval
               |> lift
          
          let contains interval : FreeDA<bool> =
               FreeAccessor.Free.contains interval
               |> lift
          
          let put interval (x : T) : FreeDA<unit> =
               FreeAccessor.Free.put interval x
               |> lift
          
          let truncate interval : FreeDA<unit> =
               FreeAccessor.Free.truncate interval
               |> lift
          
          let delete interval : FreeDA<unit> =
               FreeAccessor.Free.delete interval
               |> lift
     
     module Fund =
          type T = Fund
          let  K = Fund
          
          let lift (acc : FreeAccessor.FreeAccessor<Interval, T,'a>) : FreeDA<'a> =
               acc |> K |> liftADA 
          
          let get interval : FreeDA<T> =
               FreeAccessor.Free.get interval
               |> lift
          
          let tryGet interval : FreeDA<T option> =
               FreeAccessor.Free.tryGet interval
               |> lift
          
          let contains interval : FreeDA<bool> =
               FreeAccessor.Free.contains interval
               |> lift
          
          let put interval (x : T) : FreeDA<unit> =
               FreeAccessor.Free.put interval x
               |> lift
          
          let truncate interval : FreeDA<unit> =
               FreeAccessor.Free.truncate interval
               |> lift
          
          let delete interval : FreeDA<unit> =
               FreeAccessor.Free.delete interval
               |> lift
     
     module Winner =
          type T = Consensus.Types.Winner
          let  K = Winner
          
          let lift (acc : FreeAccessor.FreeAccessor<Interval, T,'a>) : FreeDA<'a> =
               acc |> K |> liftADA 
          
          let get interval : FreeDA<T> =
               FreeAccessor.Free.get interval
               |> lift
          
          let tryGet interval : FreeDA<T option> =
               FreeAccessor.Free.tryGet interval
               |> lift
          
          let contains interval : FreeDA<bool> =
               FreeAccessor.Free.contains interval
               |> lift
          
          let put interval (x : T) : FreeDA<unit> =
               FreeAccessor.Free.put interval x
               |> lift
          
          let truncate interval : FreeDA<unit> =
               FreeAccessor.Free.truncate interval
               |> lift
          
          let delete interval : FreeDA<unit> =
               FreeAccessor.Free.delete interval
               |> lift

     module Allocation =
          type T = Allocation
          let  K = Allocation
          
          let lift (acc : FreeAccessor.FreeAccessor<Interval, T,'a>) : FreeDA<'a> =
               acc |> K |> liftADA 
          
          let get interval : FreeDA<T> =
               FreeAccessor.Free.get interval
               |> lift
          
          let tryGet interval : FreeDA<T option> =
               FreeAccessor.Free.tryGet interval
               |> lift
          
          let contains interval : FreeDA<bool> =
               FreeAccessor.Free.contains interval
               |> lift
          
          let put interval (x : T) : FreeDA<unit> =
               FreeAccessor.Free.put interval x
               |> lift
          
          let truncate interval : FreeDA<unit> =
               FreeAccessor.Free.truncate interval
               |> lift
          
          let delete interval : FreeDA<unit> =
               FreeAccessor.Free.delete interval
               |> lift