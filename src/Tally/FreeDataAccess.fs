module Tally.FreeDataAccess

open DataAccess
open Types



type AbstractAccessor<'t, 'r> =
     | Get      of ('t -> 'r)
     | TryGet   of ('t option -> 'r)
     | Contains of (bool -> 'r)
     | Put      of ('t * 'r) 
     | Truncate of ('r)
     | Delete   of ('r)

module AbstractAccessor =
    
     let map (f : 'a -> 'b) (aa : AbstractAccessor<'t,'a>) : AbstractAccessor<'t,'b> =
          match aa with
          | Get      g      -> Get (f << g)
          | TryGet   g      -> TryGet (f << g)
          | Contains (p)    -> Contains (f << p)
          | Put      (s, x) -> Put (s, f x)  
          | Truncate (x)    -> Truncate (f x)
          | Delete   (x)    -> Delete (f x)

type Accessor<'r> =
     | Tip          of            AbstractAccessor<Tip                   , 'r>
     | PKBalance    of Interval * AbstractAccessor<PKBalance             , 'r>
     | PKAllocation of Interval * AbstractAccessor<PKAllocation          , 'r>
     | VoteUtxo     of Interval * AbstractAccessor<VoteUtxo              , 'r>
     | PKPayout     of Interval * AbstractAccessor<PKPayout              , 'r>
     | Fund         of Interval * AbstractAccessor<Fund                  , 'r>
     | Winner       of Interval * AbstractAccessor<Consensus.Types.Winner, 'r>
     | Allocation   of Interval * AbstractAccessor<Tally.Types.Allocation, 'r>

module Accessor =
     
     let map (f : 'a -> 'b) (acc : Accessor<'a>) : Accessor<'b> =
          match acc with
          | Tip          aa             -> Tip          (          AbstractAccessor.map f aa)
          | PKBalance    (interval, aa) -> PKBalance    (interval, AbstractAccessor.map f aa)
          | PKAllocation (interval, aa) -> PKAllocation (interval, AbstractAccessor.map f aa)
          | VoteUtxo     (interval, aa) -> VoteUtxo     (interval, AbstractAccessor.map f aa)
          | PKPayout     (interval, aa) -> PKPayout     (interval, AbstractAccessor.map f aa)
          | Fund         (interval, aa) -> Fund         (interval, AbstractAccessor.map f aa)
          | Winner       (interval, aa) -> Winner       (interval, AbstractAccessor.map f aa)
          | Allocation   (interval, aa) -> Allocation   (interval, AbstractAccessor.map f aa)

type FreeAccessor<'a> =
     | PureAcc of 'a
     | FreeAcc of Accessor<FreeAccessor<'a>>

let rec bind (f : 'a -> FreeAccessor<'b>) (m : FreeAccessor<'a>) : FreeAccessor<'b> =
     match m with
     | PureAcc x -> f x
     | FreeAcc t -> FreeAcc (Accessor.map (bind f) t)

let ret = PureAcc

let join (mm : FreeAccessor<FreeAccessor<'a>>) : FreeAccessor<'a> =
     bind id mm

let map (f : 'a -> 'b) (m : FreeAccessor<'a>) : FreeAccessor<'b> =
     bind (f >> ret) m

let liftAccessor (acc : Accessor<'a>) : FreeAccessor<'a> =
    FreeAcc (Accessor.map PureAcc acc)

type FreeAccessorBuilder() =
    
     member this.Bind(m : FreeAccessor<'a>, f : 'a -> FreeAccessor<'b>) : FreeAccessor<'b> =
          bind f m
     
     member this.Return(x : 'a) : FreeAccessor<'a> =
          PureAcc x
     
     member this.ReturnFrom(x : FreeAccessor<'a>) : FreeAccessor<'a> =
          x
     
     member this.Combine(m1 : FreeAccessor<'a>, m2 : FreeAccessor<'a>) : FreeAccessor<'a> =
          m1 |> bind (fun _ -> m2)
     
     member this.Zero() : FreeAccessor<unit> =
          PureAcc ()
     
     member this.Delay(f) : FreeAccessor<'a> =
          f ()

let freeAccessor = new FreeAccessorBuilder()

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

let nothing = ret ()

let inline ap f m = f <*> m

let inline lift0 x = ret x 

let inline lift1 f x = f <!> x

let inline lift2 f x y = f <!> x <*> y

let inline lift3 f x y z = f <!> x <*> y <*> z

let inline lift4 f x y z w = f <!> x <*> y <*> z <*> w

let foldM (f : 'state -> 't -> FreeAccessor<'state>) (s : 'state) : seq<'t> -> FreeAccessor<'state> = 
     Seq.fold (fun acc t -> acc >>= fun y -> f y t) (ret s)

let rec sequence (xs : FreeAccessor<'a> list) : FreeAccessor<'a list> =
     let cons x xs = x::xs
     match xs with
     | [] ->
          ret []
     | hd :: tl ->
          cons <!> hd <*> sequence tl 

let inline mapM f x = sequence (List.map f x)

let iter (f : 'a -> FreeAccessor<unit>) (xs : 'a list) : FreeAccessor<unit> =
     mapM f xs >>= (fun _ -> nothing)

module Access =
     
     module Abstract =
          
          let get : AbstractAccessor<'t, 't> =
               Get id
          
          let tryGet : AbstractAccessor<'t, 't option> =
               TryGet id
          
          let contains : AbstractAccessor<'t, bool> =
               Contains id
          
          let put (x : 't) : AbstractAccessor<'t, unit> =
               Put (x, ())
          
          let truncate : AbstractAccessor<'t, unit> =
               Truncate ()
          
          let delete : AbstractAccessor<'t, unit> =
               Delete ()

     module Tip =
          
          type T = Tip
          let  K = Tip
          
          let lift x = x |> K |> liftAccessor
          
          let get : FreeAccessor<T> =
               lift Abstract.get
          
          let tryGet : FreeAccessor<T option> =
               lift Abstract.tryGet
          
          let contains : FreeAccessor<bool> =
               lift Abstract.contains
          
          let put (x : T) : FreeAccessor<unit> =
               lift (Abstract.put x)
          
          let truncate : FreeAccessor<unit> =
               lift Abstract.truncate
          
          let delete : FreeAccessor<unit> =
               lift Abstract.delete
     
     module PKBalance =
               
          type T = PKBalance
          let  K = PKBalance
          
          let lift interval x = (interval, x) |> K |> liftAccessor
          
          let get interval : FreeAccessor<T> =
               lift interval Abstract.get
          
          let tryGet interval : FreeAccessor<T option> =
               lift interval Abstract.tryGet
          
          let contains interval : FreeAccessor<bool> =
               lift interval Abstract.contains
          
          let put interval (x : T) : FreeAccessor<unit> =
               lift interval (Abstract.put x)
          
          let truncate interval : FreeAccessor<unit> =
               lift interval Abstract.truncate
          
          let delete interval : FreeAccessor<unit> =
               lift interval Abstract.delete
     
     module PKAllocation =
                    
          type T = PKAllocation
          let  K = PKAllocation
          
          let lift interval x = (interval, x) |> K |> liftAccessor
          
          let get interval : FreeAccessor<T> =
               lift interval Abstract.get
          
          let tryGet interval : FreeAccessor<T option> =
               lift interval Abstract.tryGet
          
          let contains interval : FreeAccessor<bool> =
               lift interval Abstract.contains
          
          let put interval (x : T) : FreeAccessor<unit> =
               lift interval (Abstract.put x)
          
          let truncate interval : FreeAccessor<unit> =
               lift interval Abstract.truncate
          
          let delete interval : FreeAccessor<unit> =
               lift interval Abstract.delete
     
     module VoteUtxo =
                    
          type T = VoteUtxo
          let  K = VoteUtxo
          
          let lift interval x = (interval, x) |> K |> liftAccessor
          
          let get interval : FreeAccessor<T> =
               lift interval Abstract.get
          
          let tryGet interval : FreeAccessor<T option> =
               lift interval Abstract.tryGet
          
          let contains interval : FreeAccessor<bool> =
               lift interval Abstract.contains
          
          let put interval (x : T) : FreeAccessor<unit> =
               lift interval (Abstract.put x)
          
          let truncate interval : FreeAccessor<unit> =
               lift interval Abstract.truncate
          
          let delete interval : FreeAccessor<unit> =
               lift interval Abstract.delete
     
     module PKPayout =
                    
          type T = PKPayout
          let  K = PKPayout
          
          let lift interval x = (interval, x) |> K |> liftAccessor
          
          let get interval : FreeAccessor<T> =
               lift interval Abstract.get
          
          let tryGet interval : FreeAccessor<T option> =
               lift interval Abstract.tryGet
          
          let contains interval : FreeAccessor<bool> =
               lift interval Abstract.contains
          
          let put interval (x : T) : FreeAccessor<unit> =
               lift interval (Abstract.put x)
          
          let truncate interval : FreeAccessor<unit> =
               lift interval Abstract.truncate
          
          let delete interval : FreeAccessor<unit> =
               lift interval Abstract.delete
     
     module Fund =
                    
          type T = Fund
          let  K = Fund
          
          let lift interval x = (interval, x) |> K |> liftAccessor
          
          let get interval : FreeAccessor<T> =
               lift interval Abstract.get
          
          let tryGet interval : FreeAccessor<T option> =
               lift interval Abstract.tryGet
          
          let contains interval : FreeAccessor<bool> =
               lift interval Abstract.contains
          
          let put interval (x : T) : FreeAccessor<unit> =
               lift interval (Abstract.put x)
          
          let truncate interval : FreeAccessor<unit> =
               lift interval Abstract.truncate
          
          let delete interval : FreeAccessor<unit> =
               lift interval Abstract.delete
     
     module Winner =
                    
          type T = Consensus.Types.Winner
          let  K = Winner
          
          let lift interval x = (interval, x) |> K |> liftAccessor
          
          let get interval : FreeAccessor<T> =
               lift interval Abstract.get
          
          let tryGet interval : FreeAccessor<T option> =
               lift interval Abstract.tryGet
          
          let contains interval : FreeAccessor<bool> =
               lift interval Abstract.contains
          
          let put interval (x : T) : FreeAccessor<unit> =
               lift interval (Abstract.put x)
          
          let truncate interval : FreeAccessor<unit> =
               lift interval Abstract.truncate
          
          let delete interval : FreeAccessor<unit> =
               lift interval Abstract.delete
     
     module Allocation =
                    
          type T = Allocation
          let  K = Allocation
          
          let lift interval x = (interval, x) |> K |> liftAccessor
          
          let get interval : FreeAccessor<T> =
               lift interval Abstract.get
          
          let tryGet interval : FreeAccessor<T option> =
               lift interval Abstract.tryGet
          
          let contains interval : FreeAccessor<bool> =
               lift interval Abstract.contains
          
          let put interval (x : T) : FreeAccessor<unit> =
               lift interval (Abstract.put x)
          
          let truncate interval : FreeAccessor<unit> =
               lift interval Abstract.truncate
          
          let delete interval : FreeAccessor<unit> =
               lift interval Abstract.delete


module Compute =
     
     open Consensus.Types
     
     module private Tip =
          module T = Tip
          type   T = Tip
          let compute (computeFree : FreeAccessor<'r> -> 'r) (repo : DataAccess.T) (session : Session) (aa : AbstractAccessor<T, FreeAccessor<'r>>) : 'r =
               match aa with
               | Get      g      -> T.get      repo session |> g |> computeFree
               | TryGet   g      -> T.tryGet   repo session |> g |> computeFree
               | Contains (_)    -> failwith "Tip.contains not implemented"
               | Put      (s, x) -> T.put      repo session s ; computeFree x
               | Truncate (_)    -> failwith "Tip.truncate not implemented"
               | Delete   (_)    -> failwith "Tip.delete not implemented"
     
     module private PKBalance =
          module T = PKBalance
          type   T = PKBalance
          let compute (computeFree : FreeAccessor<'r> -> 'r) interval (repo : DataAccess.T) (session : Session) (aa : AbstractAccessor<T, FreeAccessor<'r>>) : 'r =
               match aa with
               | Get      g      -> T.get      repo session interval |> g |> computeFree
               | TryGet   g      -> T.tryGet   repo session interval |> g |> computeFree
               | Contains (p)    -> T.contains repo session interval |> p |> computeFree
               | Put      (s, x) -> T.put      repo session interval s ; computeFree x
               | Truncate (x)    -> T.truncate repo session            ; computeFree x
               | Delete   (x)    -> T.delete   repo session interval   ; computeFree x
     
     module private PKAllocation =
          module T = PKAllocation
          type   T = PKAllocation
          let compute (computeFree : FreeAccessor<'r> -> 'r) interval (repo : DataAccess.T) (session : Session) (aa : AbstractAccessor<T, FreeAccessor<'r>>) : 'r =
               match aa with
               | Get      g      -> T.get      repo session interval |> g |> computeFree
               | TryGet   g      -> T.tryGet   repo session interval |> g |> computeFree
               | Contains (p)    -> T.contains repo session interval |> p |> computeFree
               | Put      (s, x) -> T.put      repo session interval s ; computeFree x
               | Truncate (x)    -> T.truncate repo session            ; computeFree x
               | Delete   (x)    -> T.delete   repo session interval   ; computeFree x
     
     module private VoteUtxo =
          module T = VoteUtxoSet
          type   T = VoteUtxo
          let compute (computeFree : FreeAccessor<'r> -> 'r) interval (repo : DataAccess.T) (session : Session) (aa : AbstractAccessor<T, FreeAccessor<'r>>) : 'r =
               match aa with
               | Get      g      -> T.get      repo session interval |> g |> computeFree
               | TryGet   g      -> T.tryGet   repo session interval |> g |> computeFree
               | Contains (p)    -> T.contains repo session interval |> p |> computeFree
               | Put      (s, x) -> T.put      repo session interval s ; computeFree x
               | Truncate (x)    -> T.truncate repo session            ; computeFree x
               | Delete   (x)    -> T.delete   repo session interval   ; computeFree x
     
     module private PKPayout =
          module T = PKPayout
          type   T = PKPayout
          let compute (computeFree : FreeAccessor<'r> -> 'r) interval (repo : DataAccess.T) (session : Session) (aa : AbstractAccessor<T, FreeAccessor<'r>>) : 'r =
               match aa with
               | Get      g      -> T.get      repo session interval |> g |> computeFree
               | TryGet   g      -> T.tryGet   repo session interval |> g |> computeFree
               | Contains (p)    -> T.contains repo session interval |> p |> computeFree
               | Put      (s, x) -> T.put      repo session interval s ; computeFree x
               | Truncate (x)    -> T.truncate repo session            ; computeFree x
               | Delete   (x)    -> T.delete   repo session interval   ; computeFree x
     
     module private Fund =
          module T = Fund
          type   T = Fund
          let compute (computeFree : FreeAccessor<'r> -> 'r) interval (repo : DataAccess.T) (session : Session) (aa : AbstractAccessor<T, FreeAccessor<'r>>) : 'r =
               match aa with
               | Get      g      -> T.get      repo session interval |> g |> computeFree
               | TryGet   g      -> T.tryGet   repo session interval |> g |> computeFree
               | Contains (p)    -> T.contains repo session interval |> p |> computeFree
               | Put      (s, x) -> T.put      repo session interval s ; computeFree x
               | Truncate (x)    -> T.truncate repo session            ; computeFree x
               | Delete   (x)    -> T.delete   repo session interval   ; computeFree x
     
     module private Winner =
          module T = Winner
          type   T = Winner
          let compute (computeFree : FreeAccessor<'r> -> 'r) interval (repo : DataAccess.T) (session : Session) (aa : AbstractAccessor<T, FreeAccessor<'r>>) : 'r =
               match aa with
               | Get      g      -> T.get      repo session interval |> g |> computeFree
               | TryGet   g      -> T.tryGet   repo session interval |> g |> computeFree
               | Contains (p)    -> T.contains repo session interval |> p |> computeFree
               | Put      (s, x) -> T.put      repo session interval s ; computeFree x
               | Truncate (x)    -> T.truncate repo session            ; computeFree x
               | Delete   (x)    -> T.delete   repo session interval   ; computeFree x
     
     module private Allocation =
          module T = Allocation
          type   T = Allocation
          let compute (computeFree : FreeAccessor<'r> -> 'r) interval (repo : DataAccess.T) (session : Session) (aa : AbstractAccessor<T, FreeAccessor<'r>>) : 'r =
               match aa with
               | Get      g      -> T.get      repo session interval |> g |> computeFree
               | TryGet   g      -> T.tryGet   repo session interval |> g |> computeFree
               | Contains (p)    -> T.contains repo session interval |> p |> computeFree
               | Put      (s, x) -> T.put      repo session interval s ; computeFree x
               | Truncate (x)    -> T.truncate repo session            ; computeFree x
               | Delete   (x)    -> T.delete   repo session interval   ; computeFree x
     
     let rec compute (repo : DataAccess.T) (session : Session) (m : FreeAccessor<'r>) : 'r =
          let this = compute repo session
          match m with
          | FreeAcc acc ->
               match acc with
               | Accessor.Tip          aa             -> Tip          .compute this          repo session aa
               | Accessor.PKBalance    (interval, aa) -> PKBalance    .compute this interval repo session aa
               | Accessor.PKAllocation (interval, aa) -> PKAllocation .compute this interval repo session aa
               | Accessor.VoteUtxo     (interval, aa) -> VoteUtxo     .compute this interval repo session aa
               | Accessor.PKPayout     (interval, aa) -> PKPayout     .compute this interval repo session aa
               | Accessor.Fund         (interval, aa) -> Fund         .compute this interval repo session aa
               | Accessor.Winner       (interval, aa) -> Winner       .compute this interval repo session aa
               | Accessor.Allocation   (interval, aa) -> Allocation   .compute this interval repo session aa
          | PureAcc x ->
               x
     
     let Tip          repo session = Tip          .compute (compute repo session)
     let PKBalance    repo session = PKBalance    .compute (compute repo session)
     let PKAllocation repo session = PKAllocation .compute (compute repo session)
     let VoteUtxo     repo session = VoteUtxo     .compute (compute repo session)
     let PKPayout     repo session = PKPayout     .compute (compute repo session)
     let Fund         repo session = Fund         .compute (compute repo session)
     let Winner       repo session = Winner       .compute (compute repo session)
     let Allocation   repo session = Allocation   .compute (compute repo session)