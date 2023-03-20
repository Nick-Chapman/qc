module Top (main) where

import Data.List (intercalate)
import Data.Map (Map)
import Par4 (parse,separated,terminated,alts,many,sat,nl,lit)
import System.Environment (getArgs)
import Text.Printf (printf)
import qualified Data.Map as Map

main :: IO ()
main = do
  args <- getArgs
  let Config {exampleName,mode} = parseArgs args
  let q = getExample exampleName
  case mode of
    Interpret -> do
      t <- evalQI q
      putStr (prettyT t)
    CompileAndPrint -> do
      let a = compile q
      print a
    CompileAndRun -> do
      let a = compile q
      putStrLn (prettyCols (unSchema (schemaOfQuery q)))
      runI (runActionI a)
    CompilePrintAndRun -> do
      let a = compile q
      print a
      putStrLn (prettyCols (unSchema (schemaOfQuery q)))
      runI (runActionI a)
  where
    getExample :: String -> Query
    getExample x = maybe (error ("no example: "++ x)) id $ Map.lookup x examples

data Config = Config { mode :: Mode, exampleName :: String }

data Mode = Interpret | CompileAndRun | CompileAndPrint | CompilePrintAndRun

parseArgs :: [String] -> Config
parseArgs = loop Config { mode = CompilePrintAndRun
                        , exampleName = "dev"
                        }
  where
    loop :: Config -> [String] -> Config
    loop acc = \case
      [] -> acc
      "--interpret":xs -> loop acc { mode = Interpret } xs
      "--compile-and-run":xs -> loop acc { mode = CompileAndRun } xs
      "--compile-and-print":xs -> loop acc { mode = CompileAndPrint } xs
      "--compile-print-and-run":xs -> loop acc { mode = CompilePrintAndRun } xs
      flag@('-':_):_ -> do
        error (show ("unknown flag",flag))
      x:xs -> do
        loop acc { exampleName = x } xs

----------------------------------------------------------------------
-- example queries

examples :: Map String Query
examples = Map.fromList
  [ ("everything",
      ScanFile mps "data/mps.csv")

  , ("everythingAboutJohns",
      (Filter (PredEq (RefValue (VString "John")) (RefField "Forename"))
       (ScanFile mps "data/mps.csv")))

  , ("johns",
      project ["Forename","Surname","Party"]
      (Filter (PredEq (RefValue (VString "John")) (RefField "Forename"))
       (ScanFile mps "data/mps.csv")))

  , ("johnsByParty",
      project ["Party","G.Surname"]
      (ExpandAgg "G"
       (GroupBy ["Party"] "G"
        (project ["Forename","Surname","Party"]
         (Filter (PredEq (RefValue (VString "John")) (RefField "Forename"))
          (ScanFile mps "data/mps.csv"))))))

  -- example of slow quadratic join
  , ("sameSurname",
      project ["R.Surname","R.Forename","S.Forename","R.Party","S.Party"]
      (Filter
       (PredAnd
        (PredEq (RefField "R.Surname") (RefField "S.Surname"))
        (PredNe (RefField "R.Forename") (RefField "S.Forename")))
        (Join
          (projectPrefix "R" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv"))
          (projectPrefix "S" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv")))))

  -- same example using hash join
  , ("sameSurnameH",
      project ["R.Surname","R.Forename","S.Forename","R.Party","S.Party"]
      (Filter
       (PredNe (RefField "R.Forename") (RefField "S.Forename"))
        (HashJoin ["R.Surname"] ["S.Surname"]
          (projectPrefix "R" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv"))
          (projectPrefix "S" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv")))))

  , ("partyMemberCount",
      project ["Party","#members"]
      (CountAgg "G" "#members"
       (GroupBy ["Party"] "G"
         (ScanFile mps "data/mps.csv"))))

  , ("commonName",
      project ["Forename","#Forename"]
      (Filter (PredGt (RefField "#Forename") (RefValue (VInt 6)))
       (CountAgg "G" "#Forename"
        (GroupBy ["Forename"] "G"
          (ScanFile mps "data/mps.csv")))))

  , ("commonNameAcrossParties",
      project ["Forename","F.Party","F.#ForenameByParty"]
      (ExpandAgg "F"
        (Filter (PredGt (RefField "#PartiesWithThanCommonName") (RefValue (VInt 1)))
         (CountAgg "F" "#PartiesWithThanCommonName"
          (GroupBy ["Forename"] "F"
           (Filter (PredGt (RefField "#ForenameByParty") (RefValue (VInt 4)))
            (CountAgg "FP" "#ForenameByParty"
             (GroupBy ["Forename","Party"] "FP"
              (ScanFile mps "data/mps.csv")))))))))

  -- Based on same surname example, but with a final filter.
  -- "who has the same surname as a John?"
  , ("dev1",
      ProjectAs ["S.Forename"
                ,"R.Surname" -- S.Surname would be bug; field projected away.
                ] ["Forename","Surname"]
      (Filter (PredEq (RefField "R.Forename") (RefValue (VString "John")))
        (project ["R.Surname","R.Forename","S.Forename","R.Party","S.Party"]
        (Filter
         (PredAnd
          (PredEq (RefField "R.Surname") (RefField "S.Surname"))
          (PredNe (RefField "R.Forename") (RefField "S.Forename")))
         (Join
           (projectPrefix "R" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv"))
           (projectPrefix "S" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv")))))))

  , ("dev", -- based on johnsByParty, with final project removed
      ProjectAs ["P","G.E"] ["party","email"]
      (ExpandAgg "G"
       (GroupBy ["P"] "G"
        (ProjectAs ["Email","Forename","Surname","Party"] ["E","F","S","P"]
         (Filter (PredEq (RefValue (VString "John")) (RefField "Forename"))
          (ScanFile mps "data/mps.csv"))))))

  ]

  where
    project :: [ColName] -> Query -> Query
    project cols = ProjectAs cols cols

    projectPrefix :: String -> [ColName] -> Query -> Query
    projectPrefix tag cols = ProjectAs cols [ tag++"."++col | col <- cols ]

    mps = ["Forename","Surname","Name (Display as)","Name (List as)","Party","Constituency","Email","Address 1","Address 2","Postcode"]

----------------------------------------------------------------------
-- query language

data Query
  = ScanFile [ColName] FilePath
  | ProjectAs [ColName] [ColName] Query
  | Filter Pred Query
  | Join Query Query
  | HashJoin [ColName] [ColName] Query Query
  | GroupBy [ColName] ColName Query
  | ExpandAgg ColName Query
  | CountAgg ColName ColName Query
  deriving Show

data Pred
  = PredAnd Pred Pred
  | PredEq Ref Ref
  | PredNe Ref Ref
  | PredGt Ref Ref
  deriving Show

data Ref = RefValue Value | RefField ColName
  deriving Show

----------------------------------------------------------------------
-- compiling queries

schemaOfQuery :: Query -> Schema
schemaOfQuery = sofq
  where
    sofq :: Query -> Schema
    sofq = \case
      ScanFile cols _ -> mkSchema cols
      ProjectAs _ out _ -> mkSchema out
      Filter _ sub -> sofq sub
      Join subs1 subs2 ->
        combineSchema (sofq subs1) (sofq subs2)
      HashJoin{} ->
        undefined -- cols1 cols2 sub1 sub2 -> do
      GroupBy cols tag sub ->
        combineSchema (mkSchema cols) (mkSubSchema tag (sofq sub))
      ExpandAgg col sub ->
        expandAggSchema col (sofq sub)
      CountAgg _aggCol countCol sub ->
        combineSchema (sofq sub) (mkSchema [countCol])

compile :: Query -> Action
compile q = compile need0 s0 q $ \CompState{} r -> A_Emit schema r
  where
    schema = schemaOfQuery q
    need0 :: [ColName] = unSchema schema

    s0 = CompState { u = 1 }

    compile :: [ColName] -> CompState -> Query -> (CompState -> RExp -> Action) -> Action
    compile need s q k = case q of
      ScanFile cols0 filename -> do
        let cols = [ c | c <- cols0, c `elem` need ]
        genRid s $ \s rid -> do
          A_ScanFile filename rid $ do
            splitRecord cols s (RefR rid) $ \s r -> do
              k s r

      ProjectAs inp out sub -> do
        compile inp s sub $ \s r -> do -- need changes here
          k s (renameRecord inp out r)

      Filter pred sub -> do
        compile (need ++ colsOfPred pred) s sub $ \s r -> do
          A_If (compilePred r pred) (k s r)

      Join sub1 sub2 -> do
        compile need s sub1 $ \s r1 -> do
          compile need s sub2 $ \s r2 -> do
            k s (mkCombineR (r1,r2))

      HashJoin{} -> undefined -- cols1 cols2 sub1 sub2 -> do

      GroupBy cols tag sub -> do
        genHid s $ \s hid -> do
          A_NewHT hid
            (compile (need ++ cols) s sub $ \_ r -> do
                A_InsertHT hid cols r)
            (genRid s $ \s rid -> do
                (A_ScanHT hid cols tag rid
                 (k s (RefR rid))))

      ExpandAgg aggCol sub -> do
        compile need s sub $ \s r -> do
          genRid s $ \s rid -> do
            A_ExpandAgg r aggCol rid (k s (RefR rid))

      CountAgg aggCol countCol sub -> do
        compile need s sub $ \s r -> do
          genRid s $ \s rid -> do
            A_CountAgg r aggCol countCol rid (k s (RefR rid))


--_splitRecord :: [ColName] -> CompState -> RExp -> (CompState -> RExp -> Action) -> Action
--_splitRecord _ s r k = k s r

colsOfPred :: Pred -> [ColName]
colsOfPred = \case
  PredAnd p1 p2 -> colsOfPred p1 ++ colsOfPred p2
  PredEq x1 x2 -> colsOfRef x1 ++ colsOfRef x2
  PredNe x1 x2 -> colsOfRef x1 ++ colsOfRef x2
  PredGt x1 x2 -> colsOfRef x1 ++ colsOfRef x2

colsOfRef :: Ref -> [ColName]
colsOfRef = \case
  RefValue _ -> []
  RefField c -> [c]


splitRecord :: [ColName] -> CompState -> RExp -> (CompState -> RExp -> Action) -> Action
splitRecord cols s r k = loop s [] cols
  where
    loop :: CompState -> [(ColName,VExp)] -> [ColName] -> Action
    loop s acc = \case
      [] -> k s (ColWise (Map.fromList acc))
      col:cols -> do
        genVid s $ \s vid -> do
          A_LetVid vid (VSelect r col) $
            loop s ((col,VRef vid):acc) cols

genVid :: CompState -> (CompState -> VId -> Action) -> Action
genVid s@CompState{u} k = do
  k s { u = u + 1 } (VId u)

genRid :: CompState -> (CompState -> RId -> Action) -> Action
genRid s@CompState{u} k = do
  k s { u = u + 1 } (RId u)

genHid :: CompState -> (CompState -> HId -> Action) -> Action
genHid s@CompState{u} k = do
  k s { u = u + 1 } (HId u)


data CompState = CompState { u :: Int }

compilePred :: RExp -> Pred -> BExp
compilePred r = \case
  PredAnd p1 p2 -> BAnd (compilePred r p1) (compilePred r p2)
  PredEq x1 x2 -> BEq (compileRef r x1) (compileRef r x2)
  PredNe x1 x2 -> BNe (compileRef r x1) (compileRef r x2)
  PredGt x1 x2 -> BGt (compileRef r x1) (compileRef r x2)

compileRef :: RExp -> Ref -> VExp
compileRef r = \case
  RefValue v -> VLit v
  RefField c -> selectRecord c r


mkCombineR :: (RExp,RExp) -> RExp
mkCombineR = \case
  (ColWise m1, ColWise m2) -> ColWise (Map.union m1 m2)
  (re1,re2) -> CombineR re1 re2

renameRecord :: [ColName] -> [ColName] -> RExp -> RExp
renameRecord inp out re = do
  ColWise $ Map.fromList
    [ (oc,v)
    | (ic,oc) <- zip inp out
    , let v = selectRecord ic re
    ]

selectRecord :: ColName -> RExp -> VExp
selectRecord col = \case
  ColWise m ->
    maybe (error (show ("selectRecord",col))) id $ Map.lookup col m
  re ->
    VSelect re col


----------------------------------------------------------------------
-- Result of compilation: Action and {B,V,R)Exp

data Action
  = A_ScanFile FilePath RId Action
  | A_Emit Schema RExp
  | A_If BExp Action
  | A_NewHT HId Action Action
  | A_InsertHT HId [ColName] RExp
  | A_ScanHT HId [ColName] ColName RId Action
  | A_ExpandAgg RExp ColName RId Action
  | A_CountAgg RExp ColName ColName RId Action
  | A_LetVid VId VExp Action

data BExp
  = BAnd BExp BExp
  | BEq VExp VExp
  | BNe VExp VExp
  | BGt VExp VExp

data VExp
  = VLit Value
  | VSelect RExp ColName
  | VRef VId

data RExp
  = CombineR RExp RExp
--  | RenameR [ColName] [ColName] RExp
  | RefR RId
  | ColWise (Map ColName VExp) -- TODO: pick better name?

data RId
  = RId Int
  deriving (Eq,Ord)

data HId
  = HId Int
  deriving (Eq,Ord)

data VId
  = VId Int
  deriving (Eq,Ord)

----------------------------------------------------------------------
-- pp code types

instance Show Action where
  show = unlines . ppAction

ppAction :: Action -> [String]
ppAction = \case
  A_ScanFile filename rid bodyA -> concat
    [ [ "scan(" ++ show filename ++ ") { " ++ show rid ++ " =>"]
    , indent (ppAction bodyA)
    , [ "}"]
    ]
  A_Emit _sc r ->
    [ "emit: " ++ show r ] -- TODO: show schema?
  A_If p bodyA -> concat
    [ [ "if(" ++ show p ++ ") {" ]
    , indent (ppAction bodyA)
    , [ "}"]
    ]
  A_NewHT hid aBuild aScan -> concat
    [ [ "let " ++ show hid ++ " = " ++ "newHT()" ]
    , ppAction aBuild
    , ppAction aScan
    ]
  A_InsertHT hid cols r ->
    [ "insertHT: " ++ show (hid,cols,r) ]
  A_ScanHT hid cols tag rid bodyA -> concat
    [ [ "scanHT" ++ show (hid,cols,tag) ++ " { " ++ show rid ++ " =>"]
    , indent (ppAction bodyA)
    , [ "}"]
    ]
  A_ExpandAgg r col rid bodyA -> concat
    [ [ "ExpandAgg" ++ show (r,col) ++ " { " ++ show rid ++ " =>"]
    , indent (ppAction bodyA)
    , [ "}"]
    ]
  A_CountAgg r aggCol countCol rid bodyA -> concat
    [ [ "CountAgg" ++ show (r,aggCol,countCol) ++ " { " ++ show rid ++ " =>"]
    , indent (ppAction bodyA)
    , [ "}"]
    ]
  A_LetVid vid vexp action -> concat
    [ [ "let " ++ show vid ++ " = " ++ show vexp ]
    , ppAction action
    ]

  where
    indent :: [String] -> [String]
    indent xs = [ "  " ++ x | x <- xs ]

instance Show BExp where
  show = \case
    BAnd b1 b2 -> show b1 ++ " && " ++ show b2
    BEq v1 v2 -> show v1 ++ " == " ++ show v2
    BNe v1 v2 -> show v1 ++ " /= " ++ show v2
    BGt v1 v2 -> show v1 ++ " > " ++ show v2

instance Show VExp where
  show = \case
    VLit v -> show v
    VSelect r c -> "select" ++ show (r,c)
    VRef vid -> show vid

instance Show RExp where
  show = \case
    CombineR r1 r2 -> "combine" ++ show (r1,r2)
    --RenameR inp out r -> "rename" ++ show (inp,out,r)
    RefR rid -> show rid
    ColWise m -> "colwise:" ++ show (Map.toList m)

instance Show VId where
  show (VId n) = "v" ++ show n

instance Show RId where
  show (RId n) = "r" ++ show n

instance Show HId where
  show (HId n) = "h" ++ show n

----------------------------------------------------------------------
-- Action -> Interaction

runActionI :: Action -> Interaction
runActionI a = run rs0 a $ \RunState{} -> I_Done
  where
    rs0 = RunState { vm = Map.empty, rm = Map.empty, hm = Map.empty }

    run :: RunState -> Action -> (RunState -> Interaction) -> Interaction
    run s@RunState{vm,rm,hm} a k = case a of

      A_ScanFile filename rid bodyA -> do
        I_LoadTable filename $ \(Table _sc rs) -> do
          let
            inner :: RunState -> [Record] -> Interaction
            inner s = \case
              [] -> k s
              r:rs -> do
                run s { rm = Map.insert rid r rm } bodyA $ \s ->
                  inner s rs
          inner s rs

      A_Emit schema r ->
        -- TODO: need to know the schema here!
        I_Print (prettyR schema (evalR s r)) (k s)

      A_If p bodyA -> do
        if (evalB s p) then run s bodyA k else k s

      A_NewHT hid aBuild aScan  -> do
        run s { hm = Map.insert hid Map.empty hm } aBuild $ \s -> do
          run s aScan $ \s -> do
            k s

      A_InsertHT hid cols r -> do
        let h :: HT = maybe err id $ Map.lookup hid hm
              where err = error (show ("no HT for insert",hid))
        let rv :: Record = evalR s r
        let key :: [Value] = map (selectR rv) cols
        let h' :: HT = Map.insertWith (++) key [rv] h
        k s { hm = Map.insert hid h' hm }

      A_ScanHT hid cols tag rid bodyA -> do
        let
          inner :: RunState -> [([Value],[Record])] -> Interaction
          inner s = \case
            [] -> k s
            (key,bucket):rs -> do
              let rv = mkRecord (cols ++ [tag])
                                (key ++ [VAgg (Table undefined bucket) ]) -- TODO: can this undefined be provoked?
              run s { rm = Map.insert rid rv rm } bodyA $ \s ->
                inner s rs
        let h = maybe err id $ Map.lookup hid hm
              where err = error (show ("no HT for scan",hid))
        inner s (Map.toList h)

      A_ExpandAgg r col rid bodyA -> do
        let rv1 :: Record = evalR s r
        let
          inner :: RunState -> [Record] -> Interaction
          inner s = \case
            [] -> k s
            rv2:rs -> do
              let rv = combineR rv1 (prefixR col rv2)
              run s { rm = Map.insert rid rv rm } bodyA $ \s ->
                inner s rs
        let Table _sc rs = expectAgg (selectR rv1 col)
        inner s rs

      A_CountAgg r aggCol countCol rid bodyA -> do
        let rv1 :: Record = evalR s r
        let Table _sc rs = expectAgg (selectR rv1 aggCol)
        let rv2 = mkRecord [countCol] [ VInt (length rs) ]
        let rv = combineR rv1 rv2
        run s { rm = Map.insert rid rv rm } bodyA $ \s ->
          k s

      A_LetVid vid vexp action -> do
        let v = evalV s vexp
        run s { vm = Map.insert vid v vm } action $ \s ->
          k s


type HT = Map [Value] [Record]

data RunState = RunState { vm :: VMap, rm :: RMap, hm :: HMap }

type VMap = Map VId Value
type RMap = Map RId Record
type HMap = Map HId HT

evalB :: RunState -> BExp -> Bool
evalB s = \case
  BAnd b1 b2 -> evalB s b1 && evalB s b2
  BEq v1 v2 -> evalV s v1 == evalV s v2
  BNe v1 v2 -> evalV s v1 /= evalV s v2
  BGt v1 v2 -> greaterThanV (evalV s v1) (evalV s v2)

evalV :: RunState -> VExp -> Value
evalV s@RunState{vm} = \case
  VLit v -> v
  VSelect r c -> selectR (evalR s r) c
  VRef vid ->
    maybe err id $ Map.lookup vid vm
      where err = error (show ("evalV/VRef",vid))

evalR :: RunState -> RExp -> Record
evalR s@RunState{rm} = \case
  CombineR r1 r2 -> do
    combineR (evalR s r1) (evalR s r2)
  {-RenameR inp out r -> do
    renameR inp out (evalR s r)-}
  RefR rid -> do
    maybe err id $ Map.lookup rid rm
      where err = error (show ("evalR/RefR",rid))
  ColWise m ->
    Record $ Map.fromList [ (c, evalV s ve) | (c,ve) <- Map.toList m ]

----------------------------------------------------------------------
-- Interaction (instead of IO)

data Interaction
  = I_Done
  | I_LoadTable FilePath (Table -> Interaction)
  | I_Print String Interaction

runI :: Interaction -> IO ()
runI = loop Map.empty where
  loop :: Map FilePath Table -> Interaction -> IO ()
  loop tMap = \case
    I_Done -> pure ()
    I_LoadTable filename k -> do
      case Map.lookup filename tMap of
        Just tab -> do
          loop tMap (k tab)
        Nothing -> do
          tab <- loadTableFromCSV filename
          loop (Map.insert filename tab tMap) (k tab)
    I_Print s i -> do
      putStrLn s
      loop tMap i

----------------------------------------------------------------------
-- evaluating queries

evalQI :: Query -> IO Table
evalQI = eval
  where
    eval :: Query -> IO Table
    eval = \case
      ScanFile _ filename -> do
        loadTableFromCSV filename
      ProjectAs inp out sub -> do
        t <- eval sub
        pure (renameT inp out t)
      Filter pred sub -> do
        t <- eval sub
        pure (filterT (\r -> evalPred r pred) t)
      Join sub1 sub2 -> do
        t1 <- eval sub1
        t2 <- eval sub2
        pure (crossProductT t1 t2)
      HashJoin cols1 cols2 sub1 sub2 -> do
        t1 <- eval sub1
        t2 <- eval sub2
        pure (hashJoinTables cols1 cols2 t1 t2)
      GroupBy cols tag sub -> do
        t <- eval sub
        pure (groupBy cols tag t)
      ExpandAgg aggCol sub -> do
        t <- eval sub
        pure (expandAgg aggCol t)
      CountAgg aggCol countCol sub -> do
        t <- eval sub
        pure (countAgg aggCol countCol t)

evalPred :: Record -> Pred -> Bool
evalPred r = \case
  PredAnd p1 p2 -> evalPred r p1 && evalPred r p2
  PredEq x1 x2 -> evalRef r x1 == evalRef r x2
  PredNe x1 x2 -> not (evalRef r x1 == evalRef r x2)
  PredGt x1 x2 -> greaterThanV (evalRef r x1) (evalRef r x2)

evalRef :: Record -> Ref -> Value
evalRef r = \case
  RefValue v -> v
  RefField c -> selectR r c

greaterThanV :: Value -> Value -> Bool
greaterThanV v1 v2 = getIntV v1 > getIntV v2

getIntV :: Value -> Int
getIntV = \case
  VInt n -> n
  v -> error ("getIntV, not an int: " ++ show v)

----------------------------------------------------------------------
-- table operations

crossProductT :: Table -> Table -> Table
crossProductT (Table sc1 rs1) (Table sc2 rs2) =
  Table (combineSchema sc1 sc2) [ combineR r1 r2 | r1 <- rs1, r2 <- rs2 ]

hashJoinTables :: [ColName] -> [ColName] -> Table -> Table -> Table
hashJoinTables cols1 cols2 (Table sc1 rs1) (Table sc2 rs2) = do
  let m1 :: Map [Value] [Record] =
        Map.fromListWith (++)
        [ (key, [r1])
        | r1 <- rs1
        , let key = map (selectR r1) cols1
        ]
  Table (combineSchema sc1 sc2) $
    [ combineR r1 r2
    | r2 <- rs2
    , let key = map (selectR r2) cols2
            , r1 <- maybe [] id $ Map.lookup key m1
    ]

filterT :: (Record -> Bool) -> Table -> Table
filterT pred (Table schema rs) =
  Table schema [ r | r <- rs, pred r ]

renameT :: [ColName] -> [ColName] -> Table -> Table
renameT inp out (Table _sc rs) =
  Table (mkSchema out) [ renameR inp out r | r <- rs ]

groupBy :: [ColName] -> ColName -> Table -> Table
groupBy cols tag (Table _sc rs) = do
  let m1 :: Map [Value] [Record] =
        Map.fromListWith (++)
        [ (key, [r])
        | r <- rs
        , let key = map (selectR r) cols
        ]
  mkTable (combineSchema (mkSchema cols) (mkSubSchema tag _sc))
    [ k ++ [VAgg (Table undefined rs)] | (k,rs) <- Map.toList m1 ]  -- TODO: can this undefined be provoked?

expandAgg :: ColName -> Table -> Table
expandAgg aggCol (Table sc1 rs1) = do
  Table (expandAggSchema aggCol sc1) $
    [ combineR r1 (prefixR aggCol r2)
    | r1 <- rs1
    , let Table _sc2 rs2 = expectAgg (selectR r1 aggCol)
    , r2 <- rs2
    ]

countAgg :: ColName -> ColName -> Table -> Table
countAgg aggCol countCol (Table sc1 rs1) = do
  Table (combineSchema sc1 (mkSchema [countCol])) $
    [ combineR r1 rCount
    | r1 <- rs1
    , let Table _sc2 rs2 = expectAgg (selectR r1 aggCol)
    , let rCount = mkRecord [countCol] [ VInt (length rs2) ]
    ]

mkTable :: Schema  -> [[Value]] -> Table
mkTable schema vss = do
  let cols = unSchema schema
  let n = length cols
  Table schema [ mkRecord cols (checkLength n vs) | vs <- vss ]

checkLength :: Show a => Int -> [a] -> [a]
checkLength n xs =
  if (length xs == n) then xs else
    error (show ("checkLength: expected",n,"got",length xs,xs))

----------------------------------------------------------------------
-- record operations

prefixR :: String -> Record -> Record
prefixR tag (Record m) =
  Record (Map.mapKeys (\k -> tag++"."++k) m)

renameR :: [ColName] -> [ColName] -> Record -> Record
renameR inp out r =
  mkRecord out (map (selectR r) inp)

selectR :: Record -> ColName -> Value
selectR (Record m) col = do
  maybe err id $ Map.lookup col m
    where err = error (show ("select",col,Map.keys m))

combineR :: Record -> Record -> Record
combineR (Record m1) (Record m2) =
  Record (Map.union m1 m2) -- left biased for duplicate keys

----------------------------------------------------------------------
-- values

data Table = Table Schema [Record]
  deriving (Eq,Ord)

data Record = Record (Map ColName Value)
  deriving (Eq,Ord,Show)

mkRecord :: [ColName] -> [Value] -> Record
mkRecord cs vs = Record (Map.fromList (zip cs vs))

data Value = VString String | VInt Int | VAgg Table
  deriving (Eq,Ord)

expectAgg :: Value -> Table
expectAgg = \case
  VAgg table -> table
  _ -> error "expectAgg"

----------------------------------------------------------------------
-- displaying  values

instance Show Table where
  show = prettyT

instance Show Value where
  show = \case
    VString s -> show s
    VInt n -> show n
    VAgg (Table _sc rs) -> printf "[table:size=%d]" (length rs)

prettyT :: Table -> String
prettyT (Table sc rs) =
  prettyCols (unSchema sc) ++ "\n"
  ++ case rs of
       [] -> "*empty table*"
       rs -> unlines [ prettyR sc r | r <- rs ]

prettyR :: Schema -> Record -> String
prettyR sc r =
  intercalate "," [ show (selectR r c) | c <- unSchema sc ]

prettyCols :: [ColName] -> String
prettyCols cols = intercalate "," cols

----------------------------------------------------------------------
-- schema

data Schema = Schema [(ColName,Type)]
  deriving (Eq,Ord)

type ColName = String

data Type = TString | TSchema Schema
  deriving (Eq,Ord)


mkSubSchema :: ColName -> Schema -> Schema
mkSubSchema c s = Schema [(c,TSchema s)]

expandAggSchema :: ColName -> Schema -> Schema
expandAggSchema col sc =
  combineSchema sc (prefixSchema col (unfoldSubScheme sc col))

unfoldSubScheme :: Schema -> ColName -> Schema
unfoldSubScheme (Schema xs) c =
  the "unfoldSubScheme" [ unfold sc | (c',sc) <- xs, c==c' ]
  where
    unfold = \case
      TSchema sc -> sc
      TString -> error "unfold"

prefixSchema :: String -> Schema -> Schema
prefixSchema tag (Schema xs) =
  Schema [ (tag++"."++c, t) | (c,t) <- xs ]

unSchema :: Schema -> [ColName]
unSchema (Schema xs) = [ c | (c,_) <- xs ]

mkSchema :: [ColName] -> Schema
mkSchema cs = Schema [ (c,TString) | c <- cs ]

combineSchema :: Schema -> Schema -> Schema
combineSchema (Schema cs1) (Schema cs2) = Schema (cs1 ++ cs2)

instance Show Schema where
  show (Schema cs) = "<" ++ intercalate "," (map show cs) ++ ">"

instance Show Type where
  show = \case
    TString -> "*"
    TSchema sc -> show sc

----------------------------------------------------------------------
-- parsing tables from CVS

loadTableFromCSV :: FilePath -> IO Table
loadTableFromCSV filename = parseCVS <$> readFile filename

parseCVS :: String -> Table
parseCVS = parse table
  where
    table = do
      cols <- separated (lit ',') word; nl
      vss <- terminated nl record
      pure $ mkTable (mkSchema cols) vss

    record = map VString <$> separated (lit ',') word

    word = alts [quoted,unquoted]

    quoted = do
      lit '"'
      xs <- many notDoubleQuoteOrNL
      lit '"'
      pure xs

    unquoted = many notCommaOrNL

    notDoubleQuoteOrNL = sat (\case '"' -> False; '\n' -> False; _ -> True)
    notCommaOrNL = sat (\case ',' -> False; '\n' -> False; _ -> True)

----------------------------------------------------------------------
-- misc

the :: Show a => String -> [a] -> a
the who = \case [a] -> a; as -> error (show ("the",who,as))
