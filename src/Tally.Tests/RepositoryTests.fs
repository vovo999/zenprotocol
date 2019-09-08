module Tally.Tests.RepositoryTests

open Blockchain.Serialization
open NUnit.Framework
open Tally
open DataAccess
open System.IO
open Infrastructure
open Consensus
open Consensus.Serialization.Serialization
open Tally
open Types
open Tally.Tests.VotingContractCode
open System.Text

module ZRTypes = Zen.Types.Realized
module ZETypes = Zen.Types.Extracted
module ZData = Zen.Types.Data
module CryptoPublicKey = Crypto.PublicKey

let difficulty = 0x20fffffful

let rlimit = 8000000u

let result = new Infrastructure.Result.ResultBuilder<string>()
let private getBytes str = Encoding.Default.GetBytes (str : string)
let chainParams = Chain.testParameters

let votingContractId = chainParams.votingContractId

let payoutTx1 =
    Transaction.fromHex "000000000101ffae28b36d58304f685c8e7b30c6c47d44b6be457fc35ee52c3f391802e74d54000202209b95835b9af67bdc345c0ae879b93a58d0f3f90784c982a2da52fbd71d8700a3007e0000012a05f3480100000001000201620302b43a1cb4cb6472e1fcd71b237eb9c1378335cd200dd07536594348d9e450967e7cb3db0d3b466836eb754648b9653af9ce7016eec97eeef3dc1a503f8be6c32a419bf7197f83670c9c49fb75125100ac552ead13501817f13e74e038e272263102822200abbf8805a203197e4ad548e4eaa2b16f683c013e31d316f387ecf7adc65b3fb2065061796f7574010c02065061796f7574064c30323031333037353962303763613031636166386535323466633237393934366131653936616663333534366565356631666434613163666166363434373633633262343031303030303031095369676e61747572650c024230323961653962343965363032353939353833303266616236633962653333333737356664376164613732663131363433323138646366323365356633376563393209da6a1cbc8bf8ccc7a86798f0e95da563698d4dc7a06bd783875a305c26b95e6026c4353972b0fa9f924da550ffdd99bf96c020f5090c04a2c75b024f3af317904230326234336131636234636236343732653166636437316232333765623963313337383333356364323030646430373533363539343334386439653435303936376509405cf2cd5a12a4b6363f781628e7aa0590995f7bb7e18b4cbeae7cce0b631ef060d930c956d638967d6a0069543bf933e072f6ef0fc28298871b2cb0f0cc71e50301020000000000000000000005"
    |> Option.get
    |> Transaction.toExtended
let payoutTx2 =
    Transaction.fromHex "000000000101d79ab5e887493e7986755ffe09d43106bf9cb11a027cb190a22121b9347d529e0002022017ba8c88eb030b31a0da51ec5fe7bbd08ff807943f57b4cc4a33de4aca37fca6007e0000012a05f1ff01000000010002016203032229522443cf166e28468c58a4719ce01eb2d9b5b656ecae6e959001bbe8c46952bab44713303b8b08ce4bfd2203db1a929aedf9c76b46f167139c5b4cab87464d2c0fb981ea47c6eefef176db2fd26c629cfb5b7fca32bab656c91ed60c528502826700abbf8805a203197e4ad548e4eaa2b16f683c013e31d316f387ecf7adc65b3fb2065061796f7574010c02065061796f7574068010303230313334346463333433663061633664306431643564366536333838613964633439356666323330623635303536353435356630343063346162643536356331643330313830663234646233326161313838313935363634366433636362623634376466373134353564653130636639386236333538313065383837303930366135366236333830303032623637095369676e61747572650c0242303332323239353232343433636631363665323834363863353861343731396365303165623264396235623635366563616536653935393030316262653863343639098a217484c9f769d64e06abb6fc55eaf355c12c1445ccbc7ca7f35037290e4c157bbb785b2a1cc3f90527527c93644b3dfeb1bea3101ed9c616c6357310ed60594230336332376436336137613965326338353262373661656533386635316564643037663038396135336634336230383765653537383131616266363162313938623909b7c271b908f30748cd4d0d2c01d327bc340653f7d383131948f5dabec5ea231643b8d5fff8fd47a8b60d5dbcaa3a479e402c0a1dbb8d00c9063a442e535723d80301020000000000000000000005"
    |> Option.get
    |> Transaction.toExtended
let allocationTx1=
    Transaction.fromHex "000000000101ffcbf18c70139a3911f2e8b9ee3ea3f08ba0efb017917cc49b1ec6bc776e09ea000202209b95835b9af67bdc345c0ae879b93a58d0f3f90784c982a2da52fbd71d8700a3007e0000012a0604be0100000001000201620302b43a1cb4cb6472e1fcd71b237eb9c1378335cd200dd07536594348d9e450967ec1a43df834ea046c46006a8ace82c9657d6f14a9914bb25e9f21fddcd3b9d2c629d80989244a19e9ae5a90c3e819e4cfac1fe96df2cf78f0dc83edb1f7635d9b02816200abbf8805a203197e4ad548e4eaa2b16f683c013e31d316f387ecf7adc65b3fb20a416c6c6f636174696f6e010c02095369676e61747572650c02423032396165396234396536303235393935383330326661623663396265333333373735666437616461373266313136343332313864636632336535663337656339320978e1799fc18ff700c722c66ea32212dff1c005bc7d09a3accac632edb226b0a36543eada461509cea73b33f41589ba2a7a566b7e18c15c14227d444a97996a91423032623433613163623463623634373265316663643731623233376562396331333738333335636432303064643037353336353934333438643965343530393637650999617399d295eae44f0c138c26436b5fb66101aa790f4d8d09d2cd90f818b2a657b62ba9c03b3176684eb6a754d345f41de9508ff27566f6d680cc809241a1270a416c6c6f636174696f6e0604303130340301020000000000000000000005"
    |> Option.get
    |> Transaction.toExtended
let allocationTx2 =
    Transaction.fromHex "000000000101d52dcc4cef1d651035a10f75a6d0990a298becdd38649d3ba778f39b21b9620e0002022017ba8c88eb030b31a0da51ec5fe7bbd08ff807943f57b4cc4a33de4aca37fca6007e0000012a05f36501000000010002016203032229522443cf166e28468c58a4719ce01eb2d9b5b656ecae6e959001bbe8c469ddab9b41598a03fdcc5b1ed8ddbad9cef9f469cd07a4b678e8b8b1adb401579971a8e9937c74e3f2dfaf617f453d1b2bb15a1e3240b001956ce1170450a3c12102816200abbf8805a203197e4ad548e4eaa2b16f683c013e31d316f387ecf7adc65b3fb20a416c6c6f636174696f6e010c02095369676e61747572650c024230333232323935323234343363663136366532383436386335386134373139636530316562326439623562363536656361653665393539303031626265386334363909a0a7f58191dfaccf7e68a5bfbcc241b48cb73921e3611dc42c5bb25784b956ef54317935adb7430f5ac745961e748cc3e5825711baaeeec47e74994ec58e80d5423033633237643633613761396532633835326237366165653338663531656464303766303839613533663433623038376565353738313161626636316231393862390995e18de0085e04dc84eb61aa17dfcffa578c2ba7bb4d75c6f12d6a32f537c0c73883400b57c0834a146804a7abef80765cfb7315f6baa659517771d50a1e167b0a416c6c6f636174696f6e0604303130630301020000000000000000000005"
    |> Option.get
    |> Transaction.toExtended

let hashPayout = "44a69664aa2096a51e093f7ec7fb7840c570cd7ba1f86f1e67dbf0b1d34b2f17"

let generateGetUTXO utxos pt =
    match Map.tryFind pt utxos with
    | Some op -> op
    | None -> UtxoSet.NoOutput

let contractPath = NUnit.Framework.TestContext.CurrentContext.TestDirectory
    //Path.Combine [| Path.GetTempPath(); Path.GetRandomFileName() |]

let compile code = lazy (result {
    let! hints = Contract.recordHints code
    let! queries = Infrastructure.ZFStar.totalQueries hints

    let contract = {
        code = code
        hints = hints
        rlimit = rlimit
        queries = queries
    }

    return!
        Contract.compile contractPath contract
        |> Result.bind (Contract.load contractPath 100ul code)
})

let compiledVotingContract =
    lazy (Contract.load contractPath 100ul VotingContractCode.contractCode chainParams.votingContractId)
    //compile VotingContractCode.contractCode

let executeVotingContract (txSkeleton : TxSkeleton.T) context messageBody command =
    result {
        let! contract = compiledVotingContract.Force()
        
        
        let! (txSkel,_,_) = Contract.run contract txSkeleton context command Zen.Types.Main.sender.Anonymous messageBody [] None
        
        let cw = {
            contractId      = votingContractId
            command         = command
            messageBody     = messageBody
            stateCommitment = NoState
            beginInputs     = txSkeleton.pInputs |> List.length |> uint32
            beginOutputs    = txSkeleton.outputs |> List.length |> uint32
            inputsLength    = List.length txSkel.pInputs - List.length txSkeleton.pInputs |> uint32
            outputsLength   = List.length txSkel.outputs - List.length txSkeleton.outputs |> uint32
            signature       = None
            cost            = 5UL
        }
        
        let inputs =
            txSkel.pInputs
            |> List.map (function TxSkeleton.PointedOutput (pt,op) -> Outpoint pt | TxSkeleton.Mint s -> Mint s)
        
        let utxosInputs =
            txSkel.pInputs
            |> List.choose ((function TxSkeleton.PointedOutput (pt,op) -> Some (pt,op) | TxSkeleton.Mint s -> None))
            |> List.fold (fun m (pt,op) -> Map.add pt (UtxoSet.Unspent op) m) Map.empty
        
        let tx =
            {
                version   = 0u
                inputs    = inputs
                outputs   = txSkel.outputs
                witnesses = [ContractWitness cw]
                contract  = None
            }
        
        let ex = Transaction.toExtended tx
        
        return (ex, txSkel)
    }

let context0 = { blockNumber=0u; timestamp=1UL }

let createMessageBody (command : byte[]) (serBallot : byte[]) (interval : uint32) (keys : List<Crypto.KeyPair>) : ZData.data option =
    
    let serialize mkZData = mkZData >> Serialization.Data.serialize >> FsBech32.Base16.encode
    
    let ballotId = serBallot |> FsBech32.Base16.encode |> ZFStar.fsToFstString
    
    let serBallot   = serialize ZData.String ballotId 
    let serInterval = serialize ZData.U32    interval
    
    let digest = Hash.compute (String.concat "" [serInterval; serBallot] |> System.Text.Encoding.Default.GetBytes)
    
    let signatures =
        keys
        |> List.map (fun (sk, pk) -> (pk, Crypto.sign sk digest))
        |> List.map (fun (pk, sign) -> (pk |> CryptoPublicKey.toString |> ZFStar.fsToFstString, sign |> ZFStar.fsToFstSignature |> ZData.Signature))
        |> Map.ofList
        
    let msgBody =
        Map.empty
        |> Map.add command (ballotId |> ZData.String)
        |> Map.add "Signature"B ((signatures, signatures |> Map.toList |> List.length |> uint32) |> ZData.Dict |> ZData.Collection)
    
    (msgBody, msgBody |> Map.toList |> List.length |> uint32) |> ZData.Dict |> ZData.Collection |> Some

let createValidMessageBody (ballot : Ballot) (interval : uint32) (keys : List<Crypto.KeyPair>) : ZData.data option =
    
    let command = match ballot with | Allocation _ -> "Allocation"B | Payout _ -> "Payout"B
    
    let serBallot = Ballot.serialize ballot
    
    createMessageBody command serBallot interval keys

let executeValidVotingContract (ballot : Ballot) (interval : uint32) (keys : List<Crypto.KeyPair>) command  =
    let context = { context0 with blockNumber = interval * chainParams.intervalLength + chainParams.snapshot + 1u }
    executeVotingContract TxSkeleton.empty context (createValidMessageBody ballot interval keys) command 

let executeVotingContractInContext (ballot : Ballot) (interval : uint32) (keys : List<Crypto.KeyPair>) context command  =
    executeVotingContract TxSkeleton.empty context (createValidMessageBody ballot interval keys) command 

let executeVotingContractWithMessageBody (msgBody : ZData.data option) (interval : uint32) command  =
    let context = { context0 with blockNumber = interval * chainParams.intervalLength + chainParams.snapshot + 1u }
    executeVotingContract TxSkeleton.empty context msgBody command 

let ema : EMA.T =
    {
        difficulty = difficulty
        delayed    = []
    }

[<Test>]
let ``add block in repository `` () =
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = 0u; blockHash = Hash.zero}
    |> DataAccess.Tip.put dataAccess session
    
    let block:Block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = 62291u
                    difficulty  = difficulty
                    commitments = Hash.zero
                    timestamp   = 1UL
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = Hash.zero
            commitments                 = []
            transactions                = [allocationTx1]
        }
    let blockHash = Block.hash block.header

    Repository.addBlock dataAccess session chainParams blockHash block
    let interval = CGP.getInterval chainParams block.header.blockNumber
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.defaultWith ( fun() -> failwith "No PK Allocation map found" )
    |> ignore

[<Test>]
let ``a vote doesn't register if the VotingContract doesn't have neither of the "Payout" or "Allocation" command.`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Payout (ContractRecipient votingContractId, [])
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let command = "Banana"
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody (getBytes command) serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval command
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore

[<Test>]
let ``a vote payout doesn't register if outside of snapshot block`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval - 1ul }
    
    let ballot = Payout (ContractRecipient votingContractId, [])
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let command = "Payout"
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody (getBytes command) serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval command
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore

[<Test>]
let ``a vote allocation doesn't register if outside of snapshot block`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval - 1ul }
    
    let ballot = Ballot.Allocation 10uy
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let command = "Allocation"
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody (getBytes command) serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval command
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore

[<Test>]
let ``a vote registers if the VotingContract has the "Payout" command.`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Payout (ContractRecipient votingContractId, [])
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let command = "Payout"
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody (getBytes command) serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval command
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.defaultWith ( fun _ -> failwith "valid vote wasn't registered" )
    |> ignore

[<Test>]
let ``a vote registers if the VotingContract has the "Allocation" command.`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Ballot.Allocation 13uy
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let command = "Allocation"
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody (getBytes command) serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval command
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.defaultWith ( fun _ -> failwithf "[%A]" msgBody ) //"valid vote wasn't registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid vote was registered" )
    |> ignore

[<Test>]
let ``voting Payout with Allocation body`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Ballot.Allocation 13uy
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let command = "Payout"
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody (getBytes command) serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval command
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid Allocation vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid Payout vote was registered" )
    |> ignore

[<Test>]
let ``voting Allocation with Payout body`` () =
    
    let interval = 10u
    
    let context = { context0 with blockNumber = CGP.getSnapshotBlock chainParams interval + 1ul }
    
    let ballot = Payout (ContractRecipient votingContractId, [])
    
    let keys = [Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create(); Crypto.KeyPair.create()]
    
    let command = "Allocation"
    
    let serBallot = Ballot.serialize ballot
    let msgBody = createMessageBody (getBytes command) serBallot interval keys
    let res = executeVotingContractWithMessageBody msgBody interval command
    
    let ex = match res with | Ok (ex, _) -> ex | Error msg -> failwithf "Error: %s" msg
    
    let cgp : CGP.T =
        {
            allocation = 12uy
            payout     = Some (PKRecipient Hash.zero, [ { asset=Asset.defaultOf votingContractId; amount=20UL } ])
        }
    
    let acs =
        ActiveContractSet.empty
        |> ActiveContractSet.add votingContractId (compiledVotingContract.Force() |> function | Ok x -> x | _ -> failwith "no")
    
    let coinbaseTx = Block.getBlockCoinbase chainParams acs context.blockNumber [ex] Hash.zero (cgp:CGP.T)
    
    let commitments =
            Block.createCommitments Hash.zero Hash.zero (ActiveContractSet.root acs) []
            |> MerkleTree.computeRoot
    
    let block =
        {
            header =
                {
                    version     = Version0
                    parent      = Hash.zero
                    blockNumber = context.blockNumber
                    difficulty  = difficulty
                    commitments = commitments
                    timestamp   = context.timestamp
                    nonce       = (0UL, 0UL)
                }
            txMerkleRoot                = Hash.zero
            witnessMerkleRoot           = Hash.zero
            activeContractSetMerkleRoot = ActiveContractSet.root acs
            commitments                 = []
            transactions                = [coinbaseTx; ex]
        }
    
    let tempDir () =
        Path.Combine
            [| Path.GetTempPath(); Path.GetRandomFileName() |]

    let dataPath = tempDir()
    let databaseContext = DataAccess.createContext dataPath
    let dataAccess = DataAccess.init databaseContext
    use session = DatabaseContext.createSession databaseContext
    
    { blockNumber = CGP.getSnapshotBlock chainParams interval; blockHash = Hash.zero }
    |> DataAccess.Tip.put dataAccess session
    
    let blockHash = Block.hash block.header
    Repository.addBlock dataAccess session chainParams blockHash block
    
    let pkalloc = PKAllocation.tryGet dataAccess session interval
    pkalloc
    |> Option.bind ( fun pka -> pka |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid Allocation vote was registered" )
    |> ignore
    
    let pkpayout = PKPayout.tryGet dataAccess session interval
    pkpayout
    |> Option.bind ( fun pkp -> pkp |> Map.toList |> List.tryHead )
    |> Option.map ( fun _ -> failwith "invalid Payout vote was registered" )
    |> ignore
