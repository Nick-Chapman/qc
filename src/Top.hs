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
  let Config {exampleName} = parseArgs args
  let q = getExample exampleName

  let canCompile = exampleName `elem` ["johns","sameSurname","dev"]
  if | canCompile -> do
         let a = compile q
         if exampleName == "dev" then print a else pure ()
         let s = schemaOfQuery q
         putStrLn (intercalate "," s)
         runI (runActionI a)
         pure ()

     | otherwise -> do
         t <- evalQI q
         putStr (prettyT t)
  where
    getExample :: String -> Query
    getExample x = maybe (error ("no example: "++ x)) id $ Map.lookup x examples

data Config = Config { exampleName :: String }

parseArgs :: [String] -> Config
parseArgs args = do
  case args of
    [] -> Config { exampleName = default_exampleName}
    [exampleName] -> Config { exampleName }
    _ -> error (show ("args",args))
  where
    default_exampleName = "dev"

----------------------------------------------------------------------
-- example queries

examples :: Map String Query
examples = Map.fromList
  [ ("johns",
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

  , ("dev",
      --project ["Party","#members"]
      (--CountAgg "G" "#members"
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
  ]

  where
    project sc = ProjectAs sc sc
    projectPrefix tag sc = ProjectAs sc [ tag++"."++col | col <- sc ]

    mps = ["Forename","Surname","Name (Display as)","Name (List as)","Party","Constituency","Email","Address 1","Address 2","Postcode"]

----------------------------------------------------------------------
-- query language

data Query
  = ScanFile Schema FilePath -- carry schema to avoid file read at compile time
  | ProjectAs Schema Schema Query
  | Filter Pred Query
  | Join Query Query
  | HashJoin Schema Schema Query Query
  | GroupBy Schema ColName Query
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
      ScanFile sc _ -> sc
      ProjectAs _ out _ -> out
      Filter _ sub -> sofq sub
      Join subs1 subs2 -> sofq subs1 ++ sofq subs2
      HashJoin{} -> undefined -- cols1 cols2 sub1 sub2 -> do
      GroupBy cols tag _ -> cols ++ [tag]
      ExpandAgg{} -> undefined -- aggCol sub -> do
      CountAgg{} -> undefined -- aggCol countCol sub -> do

compile :: Query -> Action
compile q = compile s0 q $ \CompState{} r -> A_Emit r
  where
    s0 = CompState { u = 1 }

    compile :: CompState -> Query -> (CompState -> RExp -> Action) -> Action
    compile s q k = case q of
      ScanFile _ filename -> do
        genRid s $ \s rid ->
          A_ScanFile filename rid (k s (RefR rid))

      ProjectAs inp out sub -> do
        compile s sub $ \s r -> k s (RenameR inp out r)

      Filter pred sub -> do
        compile s sub $ \s r -> A_If (compilePred r pred) (k s r)

      Join sub1 sub2 -> do
        compile s sub1 $ \s r1 -> do
          compile s sub2 $ \s r2 -> do
            k s (CombineR r1 r2)

      HashJoin{} -> undefined -- cols1 cols2 sub1 sub2 -> do

      GroupBy cols tag sub -> do
        genHid s $ \s hid -> do
          genRid s $ \s rid -> do
            A_NewHT hid
              (compile s sub $ \_ r -> do
                  A_InsertHT cols r)
              (A_ScanHT hid tag rid
                (k s (RefR rid)))

      ExpandAgg{} -> undefined -- aggCol sub -> do
      CountAgg{} -> undefined -- aggCol countCol sub -> do


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
  PredGt{} -> undefined
{-
  PredAnd p1 p2 -> evalPred r p1 && evalPred r p2
  PredEq x1 x2 -> evalRef r x1 == evalRef r x2
  PredNe x1 x2 -> not (evalRef r x1 == evalRef r x2)
  PredGt x1 x2 -> greaterThanV (evalRef r x1) (evalRef r x2)
-}

compileRef :: RExp -> Ref -> VExp
compileRef r = \case
  RefValue v -> VLit v
  RefField c -> VSelect r c

----------------------------------------------------------------------
-- Result of compilation: Action and {B,V,R)Exp

data Action
  = A_ScanFile FilePath RId Action
  | A_Emit RExp
  | A_If BExp Action
  | A_NewHT HId Action Action
  | A_InsertHT Schema RExp
  | A_ScanHT HId ColName RId Action

data BExp
  = BAnd BExp BExp
  | BEq VExp VExp
  | BNe VExp VExp

data VExp
  = VLit Value
  | VSelect RExp ColName

data RExp
  = CombineR RExp RExp
  | RenameR Schema Schema RExp
  | RefR RId

data RId
  = RId Int
  deriving (Eq,Ord)

data HId
  = HId Int
  deriving (Eq,Ord)

----------------------------------------------------------------------
-- pp code types

instance Show Action where
  show = unlines . ppAction

ppAction :: Action -> [String]
ppAction = \case
  A_ScanFile filename rid bodyA -> concat
    [ [ "scan(" ++ show filename ++ ") { " ++ show rid ++ " => "]
    , indent (ppAction bodyA)
    , [ "}"]
    ]
  A_Emit r ->
    [ "emit: " ++ show r ]
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
  A_InsertHT schema r ->
    [ "insertHT: " ++ show (schema,r) ]
  A_ScanHT hid tag rid bodyA -> concat
    [ [ "scanHT" ++ show (hid,tag) ++ " { " ++ show rid ++ " => "]
    , indent (ppAction bodyA)
    , [ "}"]
    ]
  where
    indent :: [String] -> [String]
    indent xs = [ "  " ++ x | x <- xs ]

instance Show BExp where
  show = \case
    BAnd b1 b2 -> show b1 ++ " && " ++ show b2
    BEq v1 v2 -> show v1 ++ " == " ++ show v2
    BNe v1 v2 -> show v1 ++ " /= " ++ show v2

instance Show VExp where
  show = \case
    VLit v -> show v
    VSelect r c -> "select" ++ show (r,c)

instance Show RExp where
  show = \case
    CombineR r1 r2 -> "combine" ++ show (r1,r2)
    RenameR inp out r -> "rename" ++ show (inp,out,r)
    RefR rid -> show rid

instance Show RId where
  show (RId n) = "r" ++ show n

instance Show HId where
  show (HId n) = "h" ++ show n

----------------------------------------------------------------------
-- Action -> Interaction

runActionI :: Action -> Interaction
runActionI a = run Map.empty a I_Done
  where
    run :: RMap -> Action -> Interaction -> Interaction
    run rm a then_ = case a of

      A_ScanFile filename rid bodyA -> do
        I_LoadTable filename $ \(Table rs) -> do
          let
            inner :: [Record] -> Interaction
            inner = \case
              [] -> then_
              r:rs -> run (Map.insert rid r rm) bodyA (inner rs)
          inner rs

      A_Emit r ->
        I_Print (prettyR (evalR rm r)) then_

      A_If p bodyA -> do
        if (evalB rm p) then run rm bodyA then_ else then_

      A_NewHT{} -> do
        undefined

      A_InsertHT{} -> do
        undefined

      A_ScanHT{} -> do
        undefined

evalB :: RMap -> BExp -> Bool
evalB rm = \case
  BAnd b1 b2 -> evalB rm b1 && evalB rm b2
  BEq v1 v2 -> evalV rm v1 == evalV rm v2
  BNe v1 v2 -> evalV rm v1 /= evalV rm v2

evalV :: RMap -> VExp -> Value
evalV rm = \case
  VLit v -> v
  VSelect r c -> selectR (evalR rm r) c

evalR :: RMap -> RExp -> Record
evalR rm = \case
  CombineR r1 r2 -> do
    combineR (evalR rm r1) (evalR rm r2)
  RenameR inp out r -> do
    renameR inp out (evalR rm r)
  RefR rid -> do
    maybe err id $ Map.lookup rid rm
      where err = error (show ("evalR/RefR",rid))

type RMap = Map RId Record

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
crossProductT (Table rs1) (Table rs2) =
  Table [ combineR r1 r2 | r1 <- rs1, r2 <- rs2 ]

hashJoinTables :: Schema -> Schema -> Table -> Table -> Table
hashJoinTables cols1 cols2 (Table rs1) (Table rs2) = do
  let m1 :: Map [Value] [Record] =
        Map.fromListWith (++)
        [ (key, [r1])
        | r1 <- rs1
        , let key = map (selectR r1) cols1
        ]
  Table $
    [ combineR r1 r2
    | r2 <- rs2
    , let key = map (selectR r2) cols2
            , r1 <- maybe [] id $ Map.lookup key m1
    ]

filterT :: (Record -> Bool) -> Table -> Table
filterT pred (Table rs) =
  Table [ r | r <- rs, pred r ]

renameT :: Schema -> Schema -> Table -> Table
renameT inp out (Table rs) =
  Table [ renameR inp out r | r <- rs ]

groupBy :: Schema -> ColName -> Table -> Table
groupBy cols tag (Table rs) = do
  let _m1 :: Map [Value] [Record] =
        Map.fromListWith (++)
        [ (key, [r])
        | r <- rs
        , let key = map (selectR r) cols
        ]
  mkTable (cols ++ [tag])
    [ k ++ [VAgg (Table rs)] | (k,rs) <- Map.toList _m1 ]

expandAgg :: ColName -> Table -> Table
expandAgg aggCol (Table rs1) = do
  Table $
    [ combineR r1 (prefixR aggCol r2)
    | r1 <- rs1
    , let VAgg (Table rs2) = selectR r1 aggCol
    , r2 <- rs2
    ]

countAgg :: ColName -> ColName -> Table -> Table
countAgg aggCol countCol (Table rs1) = do
  Table $
    [ combineR r1 rCount
    | r1 <- rs1
    , let VAgg (Table rs2) = selectR r1 aggCol
    , let rCount = Record { schema = [countCol], fields = [ VInt (length rs2) ] }
    ]

----------------------------------------------------------------------
-- record operations

prefixR :: String -> Record -> Record
prefixR tag r@Record{schema} =
  renameR schema (map (\x -> tag++"."++x) schema) r

renameR :: Schema -> Schema -> Record -> Record
renameR inp out r =
  Record { fields = map (selectR r) inp, schema=out }

selectR :: Record -> ColName -> Value
selectR Record{fields,schema} col =
  the who [ v | (c,v) <- zip schema fields, c==col ]
  where who = (show ("select",col,schema))

combineR :: Record -> Record -> Record
combineR Record{fields=f1,schema=s1} Record{fields=f2,schema=s2} =
  Record{fields=f1++f2, schema=s1++s2}

----------------------------------------------------------------------
-- values

data Table = Table [Record]
  deriving (Eq,Ord)

data Record = Record { fields :: [Value], schema :: Schema }
  deriving (Eq,Ord,Show)

type Schema = [ColName]
type ColName = String

data Value = VString String | VInt Int | VAgg Table
  deriving (Eq,Ord)

----------------------------------------------------------------------
-- displaying  values

instance Show Table where
  show = prettyT

instance Show Value where
  show = \case
    VString s -> show s
    VInt n -> show n
    VAgg (Table rs) -> printf "[table:size=%d]" (length rs)

prettyT :: Table -> String
prettyT tab@(Table rs) = case rs of
   [] -> "*empty table*"
   r@Record{schema=sc1}:rs -> do
     if (not (all (== sc1) [ sc2 | Record {schema=sc2} <- rs ]))
       then error (show ( "pretty",tab)) else do
       intercalate "," sc1 ++ "\n"
         ++ unlines [ intercalate "," (map show fields) | Record {fields} <- r:rs ]

prettyR :: Record -> String
prettyR Record{fields} =
  intercalate "," (map show fields)

----------------------------------------------------------------------
-- parsing tables from CVS

loadTableFromCSV :: FilePath -> IO Table
loadTableFromCSV filename = parseCVS <$> readFile filename

parseCVS :: String -> Table
parseCVS = parse table
  where
    table = do
      sc <- separated (lit ',') word; nl
      vss <- terminated nl record
      pure $ mkTable sc vss

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

mkTable :: Schema -> [[Value]] -> Table
mkTable schema vss = do
  let n = length schema
  Table [ Record { fields = checkLength n vs
                 , schema }
        | vs <- vss ]

checkLength :: Show a => Int -> [a] -> [a]
checkLength n xs =
  if (length xs == n) then xs else
    error (show ("checkLength: expected",n,"got",length xs,xs))

the :: Show a => String -> [a] -> a
the who = \case [a] -> a; as -> error (show ("the",who,as))
