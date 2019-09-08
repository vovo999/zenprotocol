module Tally.Tests.TallyTests

open Tally.Tally
open Consensus.Types
open Consensus
open Consensus.Crypto
open NUnit.Framework

let someone = PKRecipient Hash.zero

let asset0 = Asset.defaultOf (ContractId (1u, Hash.zero))

let ``should be`` (expected : 'a) (actual : 'a) : unit =
     if expected = actual
          then ()
          else failwithf "expected: %A, actual: %A" expected actual

let genAsset (n : uint32) = Asset.defaultOf (ContractId (1u, n |> Infrastructure.BigEndianBitConverter.uint32ToBytes |> Hash.compute))

[<Test>]
let ``validateCoibaseRatio coinbase correction cap`` () =
     
     let env = {
          coinbaseCorrectionCap = CoinbaseRatio 90uy
          lowerCoinbaseBound    = CoinbaseRatio 10uy
          lastCoinbaseRatio     = CoinbaseRatio 20uy
          lastFund              = Map.empty
     }
     
     let vote = 20uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 21uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 22uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 23uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` None
     
     let vote = 24uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` None
     
     let vote = 19uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 18uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 17uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` None
     
     let vote = 16uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` None

[<Test>]
let ``validateCoibaseRatio lower coinbase bound`` () =
     
     let env = {
          coinbaseCorrectionCap = CoinbaseRatio 80uy
          lowerCoinbaseBound    = CoinbaseRatio 20uy
          lastCoinbaseRatio     = CoinbaseRatio 22uy
          lastFund              = Map.empty
     }
     
     let vote = 22uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 21uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 20uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 19uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` None
     
     let env = {
          coinbaseCorrectionCap = CoinbaseRatio 20uy
          lowerCoinbaseBound    = CoinbaseRatio 5uy
          lastCoinbaseRatio     = CoinbaseRatio 8uy
          lastFund              = Map.empty
     }
     
     let vote = 8uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 7uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 6uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 5uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 4uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` None

[<Test>]
let ``validateCoibaseRatio upper coinbase bound`` () =
     
     let env = {
          coinbaseCorrectionCap = CoinbaseRatio 80uy
          lowerCoinbaseBound    = CoinbaseRatio 20uy
          lastCoinbaseRatio     = CoinbaseRatio 98uy
          lastFund              = Map.empty
     }
     
     let vote = 99uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 100uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` (Some <| CoinbaseRatio vote)
     
     let vote = 101uy
     validateCoibaseRatio env (CoinbaseRatio vote)
     |> ``should be`` None
     

[<Test>]
let ``validatePayout tests`` () =
     
     let env = {
          coinbaseCorrectionCap = CoinbaseRatio 90uy
          lowerCoinbaseBound    = CoinbaseRatio 10uy
          lastCoinbaseRatio     = CoinbaseRatio 20uy
          lastFund              = Map.empty
     }
     
     let vote = (someone, [])
     validatePayout env vote
     |> ``should be`` (Some vote)
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}])
     validatePayout env vote
     |> ``should be`` None
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}; {asset=asset0; amount=6UL}])
     validatePayout env vote
     |> ``should be`` None
     
     let env = { env with lastFund = Map.empty |> Map.add Asset.Zen 1UL }
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}])
     validatePayout env vote
     |> ``should be`` (Some vote)
     
     let env = { env with lastFund = Map.empty |> Map.add Asset.Zen 2UL }
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}])
     validatePayout env vote
     |> ``should be`` (Some vote)
     
     let env = {
          env with
               lastFund =
               Map.empty
               |> Map.add Asset.Zen 2UL
               |> Map.add asset0 5UL
     }
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}])
     validatePayout env vote
     |> ``should be`` (Some vote)
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}; {asset=asset0; amount=1UL}])
     validatePayout env vote
     |> ``should be`` (Some vote)
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}; {asset=asset0; amount=5UL}])
     validatePayout env vote
     |> ``should be`` (Some vote)
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}; {asset=asset0; amount=6UL}])
     validatePayout env vote
     |> ``should be`` None

[<Test>]
let ``validatePayout spend with 0 amount should not be accounted for`` () =
     
     let env = {
          coinbaseCorrectionCap = CoinbaseRatio 90uy
          lowerCoinbaseBound    = CoinbaseRatio 10uy
          lastCoinbaseRatio     = CoinbaseRatio 20uy
          lastFund              = Map.empty
               |> Map.add Asset.Zen 2UL
               |> Map.add asset0 5UL
     }
     
     let vote = (someone, [{asset=Asset.Zen; amount=0UL}])
     validatePayout env vote
     |> ``should be`` None
     
     let vote = (someone, [{asset=Asset.Zen; amount=0UL}; {asset=Asset.Zen; amount=1UL}])
     validatePayout env vote
     |> ``should be`` None
     
     let vote = (someone, [{asset=Asset.Zen; amount=1UL}; {asset=Asset.Zen; amount=0UL}])
     validatePayout env vote
     |> ``should be`` None

[<Test>]
let ``validatePayout spends length cannot be bigger than 100`` () =
     
     let env = {
          coinbaseCorrectionCap = CoinbaseRatio 90uy
          lowerCoinbaseBound    = CoinbaseRatio 10uy
          lastCoinbaseRatio     = CoinbaseRatio 20uy
          lastFund              =
               List.fold (fun map n -> Map.add (genAsset (uint32 n)) 5UL map) Map.empty [1..400] 
               
     }
     
     let vote = (someone, [1..99] |> List.map (fun n -> {asset=genAsset (uint32 n); amount=1UL}))
     validatePayout env vote
     |> ``should be`` (Some vote)
     
     let vote = (someone, [1..100] |> List.map (fun n -> {asset=genAsset (uint32 n); amount=1UL}))
     validatePayout env vote
     |> ``should be`` (Some vote)
     
     let vote = (someone, [1..101] |> List.map (fun n -> {asset=genAsset (uint32 n); amount=1UL}))
     validatePayout env vote
     |> ``should be`` None
     
     let vote = (someone, [1..205] |> List.map (fun n -> {asset=genAsset (uint32 n); amount=1UL}))
     validatePayout env vote
     |> ``should be`` None

let genPk = PublicKey.fromString "02bc15ee2d0073ec3f9d0f9a61f63027e9d5b6202faed8792836c42cbdb3cd4423" |> Option.get
let genPkHash = genPk |> PublicKey.hash 

[<Test>]
let ``validate Tally`` () =
     
     let assets = genAsset 100ul
     
     let env = {
          coinbaseCorrectionCap = CoinbaseRatio 90uy
          lowerCoinbaseBound    = CoinbaseRatio 10uy
          lastCoinbaseRatio     = CoinbaseRatio 100uy
          lastFund              =
               List.fold (fun map n -> Map.add assets 500UL map) Map.empty [1..100] 
               
     }
     
     let balances =
          Map.add (genPkHash) 50UL Map.empty
     let allocationBallots =
          Map.add (genPk ) 1uy Map.empty
     let payoutBallots =
          Map.add (genPk) (PKRecipient (genPk |> PublicKey.hash), [1] |> List.map (fun n -> {asset=assets; amount=1UL *(uint64 n)})) Map.empty
     
     let tally = Tally.Tally.createTally env (balances : Map<PKHash, uint64>) (allocationBallots : Map<PK, allocation>) (payoutBallots : Map<PK, payout>)
     
     let winner:Option<Winner> = Some {payout = Some (PKRecipient (genPk |> PublicKey.hash),[{asset=assets; amount=1UL}]); allocation = Some 1uy}
     
     Tally.Tally.getWinner tally
     |> ``should be`` winner
