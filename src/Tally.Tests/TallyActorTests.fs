module Tally.Tests.TallyActorTests

open Consensus
open NUnit.Framework
open Infrastructure
open FsNetMQ

module Services = Messaging.Services

let busName = "test"
let timestamp = 1515594186383UL + 1UL
let difficulty = 0x20fffffful

[<Test>]
let ``should sync`` () =
    let databaseContext = Tally.DataAccess.createContext "test"
    let dataAccess = Tally.DataAccess.init databaseContext
    
    use session = DataAccess.DatabaseContext.createSession databaseContext
    Tally.Repository.init dataAccess session
    
    //Tally.Repository.sync dataAccess session Chain.testParameters tipBlockHash (tipHeader:BlockHeader) (getHeader:Hash -> BlockHeader) (getBlock:Hash -> Block) 
    ()