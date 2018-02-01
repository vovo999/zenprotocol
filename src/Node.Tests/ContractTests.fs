﻿module Node.Tests.ContractTests

open System
open NUnit.Framework
open FsUnit
open FsNetMQ
open Consensus
open Consensus.Types
open Infrastructure
open Consensus.ChainParameters
open Consensus
open Consensus.Tests.SampleContract
open Messaging.Services
open Messaging.Events
open Api.Types

module Actor = FsNetMQ.Actor

let busName = "test"
let chain = Chain.Test
let dataPath = ".data"
let apiUri = "127.0.0.1:29555"

let createBroker () = 
     Actor.create (fun shim ->
        use poller = Poller.create ()
        use emObserver = Poller.registerEndMessage poller shim
        
        use sbBroker = ServiceBus.Broker.create poller busName
        use evBroker = EventBus.Broker.create poller busName
        
        Actor.signal shim
        Poller.run poller           
    )

let clean () =
    if System.IO.Directory.Exists dataPath then 
        System.IO.Directory.Delete (dataPath, true)

[<OneTimeSetUp>]
let setUp = fun () ->
    clean ()
    
    createBroker () |> ignore
    Blockchain.Main.main dataPath chain busName |> ignore
    Wallet.Main.main dataPath busName chain true |> ignore
    Api.Main.main chain busName apiUri |> ignore
    
    // initialize genesis block
    let block = Block.createGenesis chain [Transaction.rootTx] (0UL,0UL)
    let client = ServiceBus.Client.create busName  
    Blockchain.validateBlock client block

[<TearDown>]
let tearDown = fun () ->
    clean ()
  
let rec waitForTx subscriber tx =
    match EventBus.Subscriber.recv subscriber with
    | TransactionAddedToMemPool (_, tx) when tx = tx -> ()
    | _ -> waitForTx subscriber tx

[<Test>]
let ``Contract should activate and execute - Bus``() = 
    let client = ServiceBus.Client.create busName  
    let subscriber = EventBus.Subscriber.create<Event> busName

    match Wallet.activateContract client sampleContractCode with 
    | ActivateContractTransactionResult.Ok (contractActivationTx, cHash) ->
        Blockchain.validateTransaction client contractActivationTx
        waitForTx subscriber contractActivationTx
        match Wallet.executeContract client cHash "" Map.empty with 
        | TransactionResult.Ok _ -> ()
        | TransactionResult.Error error ->
            failwith error 
    | ActivateContractTransactionResult.Error error ->
        failwith error 

[<Test>]
let ``Contract should activate and execute - API``() =
    let activate = new ContractActivateRequestJson.Root(sampleContractCode)        
    let response = activate.JsonValue.Request ("http://" + apiUri + "/wallet/contract/activate")
    response.StatusCode |> should equal 200
    let responseBody = 
        match response.Body with
        | FSharp.Data.Text string -> string
        | _ -> failwith "unexpected response type"
    let response' = ContractActivateResponseJson.Parse responseBody
    let execute = new ContractExecuteRequestJson.Root(response'.Address,"", [| new ContractExecuteRequestJson.Spend("", int64 0) |])
    let response = execute.JsonValue.Request ("http://" + apiUri + "/wallet/contract/execute")
    response.StatusCode |> should equal 200
    printfn "-->%A" response