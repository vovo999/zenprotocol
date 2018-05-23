module Consensus.Tests.ContractAssetTests

open Consensus
open Consensus.Types
open NUnit.Framework
open Infrastructure

let result = new Infrastructure.Result.ResultBuilder<string>()

let tempDir () =
    System.IO.Path.Combine
        [| System.IO.Path.GetTempPath(); System.IO.Path.GetRandomFileName() |]
let contractPath = tempDir()


[<Literal>]
let rlimit = 2723280u

let compile code = result {
    let! hints = Contract.recordHints code
    let! queries = Infrastructure.ZFStar.totalQueries hints

    let contract : Consensus.Types.ContractV0 = {
        code = code
        hints = hints
        rlimit = rlimit
        queries = queries
    }

    return!
        Contract.compile contractPath contract
        |> Result.bind (Contract.load contractPath 100ul code)
}

let dataPath = tempDir()

let clean() =
    Platform.cleanDirectory dataPath

[<OneTimeSetUp>]
let setUp = fun () ->
    clean()

[<TearDown>]
let tearDown = fun () ->
    clean()

let compileAndRun code =
    let stubContext:Types.ContractContext = {blockNumber=1000u;timestamp=1000000UL}
    compile code
    |> Result.bind (fun contract ->
        Contract.run contract TxSkeleton.empty stubContext "" Types.Anonymous None List.empty
        |> Result.map (fun (tx, _) -> tx)
    )

let shouldBeOk result =
    result
    |> Result.mapError failwith
    |> ignore

[<Test>]
[<ParallelizableAttribute>]
let ``Should generate assets from a string and from an int``() =
    let contractCode = """
        open Zen.Types
        open Zen.Base
        open Zen.Cost
        open Zen.Asset

        module RT = Zen.ResultT
        module Tx = Zen.TxSkeleton
        module S = FStar.String

        let main txSkeleton _ contractHash command sender data wallet =
            let str = "Test" in

            if S.length str < 29 then
            begin
                let! assetString = Zen.Asset.fromSubtypeString contractHash str in
                let! assetInt = Zen.Asset.fromSubtypeInt contractHash 9999999ul in
                let! txSkeleton =
                    Tx.mint 10UL assetInt txSkeleton
                    >>= Tx.mint 20UL assetString
                in
                RT.ok (txSkeleton, None)
            end
            else
                RT.autoFailw "unexpected"

        val cf: txSkeleton -> context -> string -> sender -> option data -> wallet -> cost nat 11
        let cf _ _ _ _ _ _ = ret (64 + (64 + (64 + 64 + 0)) + 26)
        """
    compileAndRun contractCode
    |> shouldBeOk
