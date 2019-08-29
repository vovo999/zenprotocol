module Infrastructure.List

let private random = System.Random()

let remove item = List.filter (fun x -> x <> item)

let reject fn = List.filter (fn >> not)

let add item list = List.append list [item]

let update index newValue =
    List.mapi (fun i value -> if i = index then newValue else value)

// Fisher-Yates shuffle
let shuffle list : 'a list =
    let arr = List.toArray list
    for i = Array.length arr - 1 downto 1 do
        let j = random.Next(i+1)    // 0<=j<i+1
        let v = arr.[i]
        arr.[i] <- arr.[j]
        arr.[j] <- v
    Array.toList arr

let concatMap (f : 'a -> List<'b>) : List<'a> -> List<'b> =
    List.map f >> List.concat

let sequencePair ((s,xs) : 's * List<'a>) : List<'s * 'a> =
    List.map (fun x -> (s,x)) xs 

let pairWith (s : 's) (xs : List<'a>) : List<'s * 'a> =
    sequencePair (s, xs)

let traversePair (f : 'a -> List<'b>) ((s,x) : 's * 'a) : List<'s * 'b> =
    sequencePair (s, f x)